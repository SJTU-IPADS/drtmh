#include "rocc_config.h"
#include "tx_config.h"
#include "config.h"

#include "bench_runner.h"
#include "bench_listener.h"

#include "core/tcp_adapter.hpp"
#include "core/utils/util.h"
#include "core/logging.h"

#include "rtx/logger.hpp"
#include "rtx/global_vars.h"

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <boost/algorithm/string.hpp>

using namespace std;

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

std::vector<std::string> cluster_topology;

namespace nocc {

volatile bool running;

/**
 * Global config RDMA RC based message.
 */
uint64_t total_ring_sz;
uint64_t ring_padding;
uint64_t ringsz;

RdmaCtrl *cm;

namespace oltp {

/**
 * Replication factor in the cluster.
 */
int rep_factor;

/**
 * Globally defined RDMA related data structures
 */
char *rdma_buffer = NULL;
char *store_buffer = NULL;
char *free_buffer  = NULL;

uint64_t r_buffer_size = 0;
std::string config_file_name;
std::string host_file = "hosts.xml";  // default host file

MemDB *backup_stores_[MAX_BACKUP_NUM];

/* Abstract bench runner */
BenchRunner::BenchRunner(std::string &config_file)
    : barrier_a_(1),barrier_b_(1),init_worker_count_(0),store_(NULL)
{
  running = true;

  parse_config(config_file); // should fill net_def_

  std::fill_n(backup_stores_,RTX_MAX_BACKUP,static_cast<MemDB *>(nullptr));

  rtx::global_view = new rtx::SymmetricView(rep_factor,net_def_.size());
  //rtx::global_view->print();

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
  cm = new RdmaCtrl(current_partition,r_port);

#if USE_RDMA
  cm->query_devs();
#endif

  memset(rdma_buffer,0,r_buffer_size);
  uint64_t M2 = HUGE_PAGE_SZ;

#if USE_TCP_MSG
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
#endif //

  // Calculating logger memory size
  using namespace rtx;

  volatile int ts = nthreads;
  LogMemManager log_mem(NULL,total_partition,ts,32 * RTX_LOG_ENTRY_SIZE);
  uint64_t logger_sz = log_mem.total_log_size();

  logger_sz = Round(logger_sz, M2);
  LOG(2) << "Total logger area " << get_memory_size_g(logger_sz) << "G.";

  uint64_t ring_area_sz = 0;
  uint64_t total_sz = logger_sz + ring_area_sz + M2;
  assert(r_buffer_size > total_sz);

  uint64_t store_size = 0;
#if ONE_SIDED_READ == 1
  if(1){
    store_size = RDMA_STORE_SIZE * M;
    store_buffer = rdma_buffer + total_sz;
    LOG(3) << "add RDMA store size " << get_memory_size_g(store_size) << "G.";
  }
#endif
  total_sz += store_size;

  if(total_sz <= 8 * M2)
    total_sz = 8 * M2;

  // Init rmalloc
  free_buffer = rdma_buffer + total_sz; // use the free buffer as the local RDMA heap
  LOG(3) << "First " << get_memory_size_g(total_sz) << "G are left over.";
  uint64_t real_alloced = RInit(free_buffer, r_buffer_size - total_sz);
  assert(real_alloced != 0);
  LOG(3) << "RDMA heap size " << get_memory_size_g(real_alloced) <<"G.";

  RThreadLocalInit();

  warmup_buffer(rdma_buffer);

  if(cm == NULL && net_def_.size() != 1) {
    LOG(7) << "Cannot create RDMA connection manger, or cannot find cluster setting.";
  }

  /* loading database */
  init_store(store_);

#if RECORD_STALE
  MemNode::init_time = std::chrono::system_clock::now();
#endif

  const std::vector<BenchLoader *> loaders = make_loaders(current_partition);
  {
    const std::pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
    {
      for (std::vector<BenchLoader *>::const_iterator it = loaders.begin();
           it != loaders.end(); ++it) {
        (*it)->start();
      }
      for (std::vector<BenchLoader *>::const_iterator it = loaders.begin();
           it != loaders.end(); ++it)
        (*it)->join();
    }
    const std::pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    std::cout << "[Runner] local db size: " << delta_mb << " MB" << std::endl;
  }
  init_put();

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

  std::set<int> backed_list; // the primary id which I should backed
  global_view->response_for(current_partition,backed_list);

  int i(0);LOG(5) << "backed list num: " << backed_list.size();
  for(auto it = backed_list.begin();it != backed_list.end();++it) {

    int backed_id = *it;

    init_backup_store(backup_stores_[i]);
    const std::vector<BenchLoader *> loaders = make_loaders(backed_id,backup_stores_[i]);
    {
      const std::pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
      {
        //  scoped_timer t("dataloading", verbose);
        for (std::vector<BenchLoader *>::const_iterator it = loaders.begin();
             it != loaders.end(); ++it) {
          (*it)->start();
        }
        for (std::vector<BenchLoader *>::const_iterator it = loaders.begin();
             it != loaders.end(); ++it) {
          (*it)->join();
        }
      }
      const std::pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
      const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
      const double delta_mb = double(delta)/1048576.0;
      LOG(2) << "[Runner] Backup DB[" << i << "] for " << backed_id << " size: " << delta_mb << " MB";
      i++;
    }
  }

  const std::pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
  std::vector<RWorker *> workers = make_workers();

  if(poller != NULL)
    workers.push_back(poller);

  bootstrap_with_rdma(cm);
  for (std::vector<RWorker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it){
    (*it)->start();
  }

  util::timer t;

  BenchLocalListener *l = new BenchLocalListener(nthreads + nclients + 2,
                                                workers,new BenchReporter());
  sleep(1);l->start();

  for(auto it = workers.begin();it != workers.end();++it) {
    (*it)->join();
  }

  l->join();

  // close TCP connections, if possible
  try {
    //recv_context.close();
  } catch(...) {

  }

  Adapter::close_shared_sockets();
  try {
    //send_context.close();
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

    try {
      tcp_port = pt.get<size_t>("bench.port");
    } catch (const ptree_error &e) {

    }
    LOG(2) << "Use TCP port " << tcp_port;

    try{
      nclients = pt.get<size_t>("bench.clients");
    } catch(const ptree_error &e) {
      // pass
    }
    try {
      rep_factor = pt.get<size_t>("bench.rep_factor");
    } catch (const ptree_error &e) {
      LOG(LOG_ERROR) << "parse rep_factor " << config_file
                     << " error. It may be an error, or not." << e.what();
      rep_factor = 0;
    }

    if(scale_factor == 0)
      scale_factor = nthreads;

  } catch (const ptree_error &e) {
    /* using the default settings  */
    LOG(LOG_ERROR) << "some error happens in parse scale factor or clients. Maybe its not important";
  }
  LOG(2) << "use scale factor: " << scale_factor << "; with total " << nthreads << " threads.";
  ASSERT(coroutine_num >= 1) << "use error coroutine num " << coroutine_num;

  net_def_.clear();
  net_def_ = parse_network(total_partition,host_file);
  cluster_topology = net_def_;
} // parse config parameter


} // namespace oltp

};  // namespace nocc
