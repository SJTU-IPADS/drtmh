#include "rocc_config.h"
#include "tx_config.h"
#include "config.h"

#include "bench_runner.h"
#include "bench_listener.h"

#include "core/tcp_adapter.hpp"
#include "core/utils/util.h"
#include "core/logging.h"

#include "rtx/logger.hpp"

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <boost/algorithm/string.hpp>

#include "ring_imm_msg.h"


/* global config constants */
size_t nthreads = 1;                      // total server threads used
size_t nclients = 1;                      // total client used
size_t coroutine_num = 1;                 // number of concurrent request per worker

size_t backup_nthreads = 0;                   // number of log cleaner
size_t scale_factor = 0;                  // the scale of the database
size_t total_partition = 1;               // total of machines in used
size_t current_partition = 0;             // current worker id

size_t distributed_ratio = 1; // the distributed transaction's ratio


int tcp_port = 33333;

namespace nocc {

volatile bool running;
zmq::context_t recv_context(1);
zmq::context_t send_context(1);

uint64_t total_ring_sz;
uint64_t ring_padding;
uint64_t ringsz;

AdapterPoller *poller = NULL;

extern RdmaCtrl *cm;

std::vector<SingleQueue *>   local_comm_queues;

namespace oltp {

extern int rep_factor;

char *rdma_buffer = NULL;
char *store_buffer = NULL;
char *free_buffer  = NULL;

uint64_t r_buffer_size = 0;
std::string config_file_name;
std::string host_file = "hosts.xml";  // default host file

LogHelper *logger = new LogHelper();

View *my_view = NULL;

MemDB *backup_stores_[MAX_BACKUP_NUM];


/* Abstract bench runner */
BenchRunner::BenchRunner(std::string &config_file)
    : barrier_a_(1),barrier_b_(1),init_worker_count_(0),store_(NULL)
{
  running = true;

  parse_config(config_file); // should fill net_def_
  my_view = new View(config_file,net_def_);
#if TX_USE_LOG
  my_view->print_view();
#endif
  for(int i = 0; i < MAX_BACKUP_NUM; i++){
    backup_stores_[i] = NULL;
  }


  /* reset the barrier number */
  barrier_a_.n = nthreads;
}

void
BenchRunner::run() {

  // init RDMA, basic parameters
  // DZY:do no forget to allow enough hugepages
  uint64_t M = 1024 * 1024;
  r_buffer_size = M * ROCC_RBUF_SIZE_M;

  int r_port = tcp_port;

  rdma_buffer = (char *)malloc_huge_pages(r_buffer_size,HUGE_PAGE_SZ,HUGE_PAGE);
  assert(rdma_buffer != NULL);

  // start creating RDMA
  cm = new RdmaCtrl(current_partition,net_def_,r_port,false);
  cm->set_connect_mr(rdma_buffer,r_buffer_size); // register the buffer

#if USE_RDMA
  cm->query_devinfo();
#endif

  memset(rdma_buffer,0,r_buffer_size);
  uint64_t M2 = HUGE_PAGE_SZ;

  uint64_t ring_area_sz = 0;
#if USE_UD_MSG == 0 && USE_TCP_MSG == 0
  // Calculating message size
  using namespace rdmaio::ring_imm_msg;
  ring_padding = MAX_MSG_SIZE;
  total_ring_sz = coroutine_num * (2 * MAX_MSG_SIZE + 2 * 4096)  + ring_padding + MSG_META_SZ; // used for applications
  assert(total_ring_sz < r_buffer_size);

  ringsz = total_ring_sz - ring_padding - MSG_META_SZ;

  ring_area_sz = (total_ring_sz * net_def_.size()) * (nthreads + 1);
  fprintf(stdout,"[Mem], Total msg buf area:  %fG\n",get_memory_size_g(ring_area_sz));
#elif USE_TCP_MSG
  // init TCP connections
  for(uint i = 0;i < nthreads + nclients + 4;++i) {
    local_comm_queues.push_back(new SingleQueue());
  }
  poller = new AdapterPoller(local_comm_queues,tcp_port);
  poller->create_recv_socket(recv_context);

  // create sender sockets
#if DEDICATED == 0
  Adapter::create_shared_sockets(cm->network_,tcp_port,send_context);
#endif // end if create dedicated send sockets
#endif // end if USE_RDMA

  // Calculating logger memory size
#if TX_USE_LOG == 1
  uint64_t logger_sz = DBLogger::get_memory_size(nthreads, net_def_.size());
  logger_sz = logger_sz + M2 - logger_sz % M2;
  // Set logger's global offset
  DBLogger::set_base_offset(ring_area_sz + M2);
#else
  //LogHelper::configure(rdma_buffer + M2,total_partition,nthreads,coroutine_num,MAX_MSG_SIZE);
  //uint64_t logger_sz = LogHelper::get_log_size();
  using namespace rtx;
  LogMemManager log_mem(NULL,total_partition,nthreads,(coroutine_num + 1) * RTX_LOG_ENTRY_SIZE);
  uint64_t logger_sz = log_mem.total_log_size();
#endif
  fprintf(stdout,"[Mem], Total logger area %fG\n",get_memory_size_g(logger_sz));

  uint64_t total_sz = logger_sz + ring_area_sz + M2;
  assert(r_buffer_size > total_sz);

  uint64_t store_size = 0;
#if ONE_SIDED_READ == 1
  if(1){
    store_size = RDMA_STORE_SIZE * M;
    store_buffer = rdma_buffer + total_sz;
    fprintf(stdout,"add RDMA store size %f\n",get_memory_size_g(store_size));
  }
#endif
  total_sz += store_size;

  // Init rmalloc
  free_buffer = rdma_buffer + total_sz; // use the free buffer as the local RDMA heap
  uint64_t real_alloced = RInit(free_buffer, r_buffer_size - total_sz);
  assert(real_alloced != 0);
  fprintf(stdout,"[Mem], Real rdma alloced %fG\n",get_memory_size_g(real_alloced));

  RThreadLocalInit();

  warmup_buffer(rdma_buffer);

  if(cm == NULL && net_def_.size() != 1) {
    fprintf(stdout,"Distributed transactions needs RDMA support!\n");
    exit(-1);
  }

  int num_primaries = my_view->is_primary(current_partition);
  int backups[MAX_BACKUP_NUM];
  int num_backups = my_view->is_backup(current_partition,backups);
  LOG(2) << "num primaries: "<< num_primaries << " ;num backups: " << num_backups;

  /* loading database */
  init_store(store_);

#if RECORD_STALE
  MemNode::init_time = std::chrono::system_clock::now();
#endif

  const vector<BenchLoader *> loaders = make_loaders(current_partition);
  {
    const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
    {
      //  scoped_timer t("dataloading", verbose);
      for (vector<BenchLoader *>::const_iterator it = loaders.begin();
           it != loaders.end(); ++it) {
        (*it)->start();
      }
      for (vector<BenchLoader *>::const_iterator it = loaders.begin();
           it != loaders.end(); ++it)
        (*it)->join();
    }
    const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    cout << "[Runner] local db size: " << delta_mb << " MB" << endl;
  }
  init_put();

#if USE_RDMA
  cm->start_server(); // listening server for receive QP connection requests
#endif

#if ONE_SIDED_READ == 1
  {
    // fetch if possible the cached entries from remote servers
    auto mem_info_before = get_system_memory_info();
    populate_cache();
    auto mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    cerr << "[Runner] Cache size: " << delta_mb << " MB" << endl;
  }
#endif

  for(int i = 0; i < num_backups; i++){

    assert(i < MAX_BACKUP_NUM);
    int backed_id = backups[i];

    init_backup_store(backup_stores_[i]);
    const vector<BenchLoader *> loaders = make_loaders(backups[i],backup_stores_[i]);
    {
      const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
      {
        //  scoped_timer t("dataloading", verbose);
        for (vector<BenchLoader *>::const_iterator it = loaders.begin();
             it != loaders.end(); ++it) {
          (*it)->start();
        }
        for (vector<BenchLoader *>::const_iterator it = loaders.begin();
             it != loaders.end(); ++it) {
          (*it)->join();
        }
      }
      const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
      const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
      const double delta_mb = double(delta)/1048576.0;
      cerr << "[Runner] Backup DB[" << i << "] for " << backed_id << " size: " << delta_mb << " MB" << endl;
    }
    logger->add_backup_store(backed_id,backup_stores_[i]);
  }
#if TX_USE_LOG
  if(rep_factor){
    const vector<BackupBenchWorker *> backup_workers = make_backup_workers();

    for (vector<BackupBenchWorker *>::const_iterator it = backup_workers.begin();
         it != backup_workers.end(); ++it){
      (*it)->start();
    }
  }
#endif


  const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
  vector<RWorker *> workers = make_workers();

  if(poller != NULL)
    workers.push_back(poller);

  bootstrap_with_rdma(cm);
  for (vector<RWorker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it){
    (*it)->start();
  }

  util::timer t;

  BenchLocalListener *l = new BenchLocalListener(nthreads + nclients + 2,
                                                workers,new BenchReporter());
  sleep(1);l->start();

  for(auto it = workers.begin();it != workers.end();++it) {
    (*it)->join();
    delete (*it);
  }

  //listener_->join();
  l->join();

  // close TCP connections, if possible
  try {
    recv_context.close();
  } catch(...) {

  }

  Adapter::close_shared_sockets();
  try {
    send_context.close();
  } catch(...) {

  }
  LOG(2) << "main runner ends.";
  /* end main run */
}


void BenchRunner::parse_config(std::string &config_file) {
  /* parsing the config file to git machine mapping*/
  using boost::property_tree::ptree;
  using namespace boost;
  using namespace property_tree;
  ptree pt;

  try {
    read_xml(config_file, pt);

    try {
      scale_factor   = pt.get<size_t>("bench.scale") * nthreads;
      //scale_factor   = pt.get<size_t>("bench.scale");
    } catch (const ptree_error &e) {
      LOG(LOG_ERROR) << "parse config file " << config_file
                     << "error. It may be an error, or not." << e.what();
    }

    try{
      nclients = pt.get<size_t>("bench.clients");
    } catch(const ptree_error &e) {
      // pass
    }

    if(scale_factor == 0)
      scale_factor = nthreads;

  } catch (const ptree_error &e) {
    /* using the default settings  */
    LOG(LOG_ERROR) << "some error happens in parse scale factor or clients. Maybe its not important";
  }
  LOG(2) << "use scale factor: " << scale_factor << "; with total " << nthreads << "threads.";
  ASSERT(coroutine_num >= 1) << "use error coroutine num " << coroutine_num;

  net_def_.clear();
  net_def_ = parse_network(total_partition,host_file);
} // parse config parameter


} // namespace oltp

};  // namespace nocc
