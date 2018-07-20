#include "tx_config.h"

#include "bank_schema.h"
#include "bank_worker.h"
#include "bank_log_cleaner.h"

#include "db/txs/dbsi.h"
#include "db/txs/ts_manager.hpp"

#include "framework/bench_runner.h"

#include "util/printer.h"

#include "rdmaio.h"
using namespace rdmaio;

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <string>

using namespace std;

extern size_t scale_factor;
extern size_t nthreads;
extern size_t nclients;
extern size_t current_partition;
extern size_t total_partition;
extern int verbose;

extern uint64_t ops_per_worker;

namespace nocc {

extern RdmaCtrl *cm;       // global RDMA handler
namespace oltp {

extern char *store_buffer; // the buffer used to store DrTM-kv
extern MemDB *backup_stores_[MAX_BACKUP_NUM];

namespace bank {

/* sp, dc, payment, ts, wc, aml */
//unsigned g_txn_workload_mix[6] = {25,15,15,15,15,15};
unsigned g_txn_workload_mix[6] = {0,0,100,0,0,0};

class BankClient : public BenchClient {
 public:
  BankClient(unsigned worker_id,unsigned seed) : BenchClient(worker_id,seed) {

  }
  virtual int get_workload(char *input,util::fast_random &rand) {
    // pick execute machine
    uint pid  = rand.next() % total_partition;

    // pick tx idx
    static auto workload = BankWorker::_get_workload();
    double d = rand.next_uniform();

    uint tx_idx = 0;
    for(size_t i = 0;i < workload.size();++i) {
      if((i + 1) == workload.size() || d < workload[i].frequency) {
        tx_idx = i;
        break;
      }
      d -= workload[i].frequency;
    }
    *(uint8_t *)input = tx_idx;
    return pid;
  }
};

class BankMainRunner : public BenchRunner  {
 public:
  BankMainRunner(std::string &config_file) ;
  virtual void init_put() {}
  virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB* store = NULL);
  virtual std::vector<RWorker *> make_workers();
  virtual std::vector<BackupBenchWorker *> make_backup_workers();
  virtual void init_store(MemDB* &store);
  virtual void init_backup_store(MemDB* &store);
  virtual void populate_cache();

  virtual void bootstrap_with_rdma(RdmaCtrl *r) {
  }

  virtual void warmup_buffer(char *buffer) {
  }
};


void BankTest(int argc,char **argv) {
  BankMainRunner runner (nocc::oltp::config_file_name);
  runner.run();
  return ;
}

BankMainRunner::BankMainRunner(std::string &config_file) : BenchRunner(config_file) {

  using boost::property_tree::ptree;
  using namespace boost;
  using namespace property_tree;

  // parse input xml
  try {
    // parse each TX's ratio
    ptree pt;
    read_xml(config_file,pt);

    int sp = pt.get<int> ("bench.bank.sp");
    int dc = pt.get<int> ("bench.bank.dc");
    int payment = pt.get<int> ("bench.bank.payment");
    int ts = pt.get<int> ("bench.bank.ts");
    int wc = pt.get<int> ("bench.bank.wc");
    int aml = pt.get<int> ("bench.bank.aml");

    g_txn_workload_mix[0] = sp;
    g_txn_workload_mix[1] = dc;
    g_txn_workload_mix[2] = payment;
    g_txn_workload_mix[3] = ts;
    g_txn_workload_mix[4] = wc;
    g_txn_workload_mix[5] = aml;

  } catch (const ptree_error &e) {
    //pass
  }

  fprintf(stdout,"[Bank]: check workload %u, %u, %u, %u, %u, %u\n",
          g_txn_workload_mix[0],g_txn_workload_mix[1],g_txn_workload_mix[2],g_txn_workload_mix[3],
          g_txn_workload_mix[4],g_txn_workload_mix[5]);
}

void BankMainRunner::init_store(MemDB* &store){
  assert(store == NULL);
  // Should not give store_buffer to backup MemDB!
  store = new MemDB(store_buffer);
  int meta_size = META_SIZE;

  //store->AddSchema(ACCT, TAB_HASH,sizeof(uint64_t),sizeof(account::value),meta_size);
  store->AddSchema(SAV,  TAB_HASH,sizeof(uint64_t),sizeof(savings::value),meta_size,
                   NumAccounts() / total_partition * 1.5);
  store->AddSchema(CHECK,TAB_HASH,sizeof(uint64_t),sizeof(checking::value),meta_size,
                   NumAccounts() / total_partition * 1.5);

#if ONE_SIDED_READ == 1
  //store->EnableRemoteAccess(ACCT,cm);
  store->EnableRemoteAccess(SAV,cm);
  store->EnableRemoteAccess(CHECK,cm);
#endif
}

void BankMainRunner::init_backup_store(MemDB* &store){
  assert(store == NULL);
  store = new MemDB();
  int meta_size = META_SIZE;
  // store->AddSchema(ACCT, TAB_HASH,sizeof(uint64_t),sizeof(account::value),meta_size);
  store->AddSchema(SAV,  TAB_HASH,sizeof(uint64_t),sizeof(savings::value),meta_size,
                   NumAccounts() / total_partition);
  store->AddSchema(CHECK,TAB_HASH,sizeof(uint64_t),sizeof(checking::value),meta_size,
                   NumAccounts() / total_partition);
}

class BankLoader : public BenchLoader {
  MemDB *store_;
  bool is_primary_;
 public:
  BankLoader(unsigned long seed, int partition, MemDB *store, bool is_primary) : BenchLoader(seed) {
    store_ = store;
    partition_ = partition;
    is_primary_ = is_primary;
  }

  void load() {
    printf("loading store\n");
#if ONE_SIDED_READ == 1
    if(is_primary_)
      RThreadLocalInit();
#endif

    fprintf(stdout,"[Bank], total %lu accounts loaded\n", NumAccounts());
    //int meta_size = META_SIZE;
    int meta_size = 0;

    char acct_name[32];
    const char *acctNameFormat = "%lld 32 d";

    //fprintf(stdout,"loadint .. from %d to %d\n",GetStartAcct(),GetEndAcct());
    uint64_t loaded_acct(0),loaded_hot(0);

    for(uint64_t i = 0;i <= NumAccounts();++i){

      uint64_t pid = AcctToPid(i);
      assert(0 <= pid && pid < total_partition);
      if(pid != partition_) continue;

      uint64_t round_sz = CACHE_LINE_SZ << 1; // 128 = 2 * cacheline to avoid false sharing

      char *wrapper_acct, *wrapper_saving, *wrapper_check;
#if ONE_SIDED_READ == 1
      if(is_primary_){
        wrapper_saving = (char *)Rmalloc(store_->_schemas[SAV].total_len);
        assert(wrapper_saving != NULL);
        wrapper_check  = (char *)Rmalloc(store_->_schemas[CHECK].total_len);
        assert(wrapper_check != NULL);
      } else {
        wrapper_saving = new char[meta_size + sizeof(savings::value)];
        wrapper_check  = new char[meta_size + sizeof(checking::value)];
      }
      wrapper_acct = new char[meta_size + sizeof(account::value)];
#else
      wrapper_acct = new char[meta_size + sizeof(account::value)];
      wrapper_saving = new char[meta_size + sizeof(savings::value)];
      wrapper_check  = new char[meta_size + sizeof(checking::value)];
#endif


      loaded_acct += 1;

      if(i < NumHotAccounts())
        loaded_hot += 1;

      sprintf(acct_name,acctNameFormat,i);

      memset(wrapper_acct, 0, meta_size);
      memset(wrapper_saving, 0, meta_size);
      memset(wrapper_check,  0, meta_size);

      account::value *a = (account::value*)(wrapper_acct + meta_size);
      a->a_name.assign(std::string(acct_name) );
      //store_->Put(ACCT,i,(uint64_t *)wrapper_acct);
      float balance_c = (float)_RandomNumber(random_generator_, MIN_BALANCE, MAX_BALANCE);
      float balance_s = (float)_RandomNumber(random_generator_, MIN_BALANCE, MAX_BALANCE);

      savings::value *s = (savings::value *)(wrapper_saving + meta_size);
      s->s_balance = balance_s;
      store_->Put(SAV,i,(uint64_t *)wrapper_saving);
      MemNode *node = store_->stores_[SAV]->Get(i);
      assert(node->seq == 2);

      checking::value *c = (checking::value *)(wrapper_check + meta_size);

      c->c_balance = balance_c;
      //c->c_balance = i + 73;
      assert(c->c_balance > 0);
      store_->Put(CHECK,i,(uint64_t *)wrapper_check);
      node = store_->stores_[CHECK]->Get(i);
#if INLINE_OVERWRITE
      assert(sizeof(checking::value) <= 16);
      memcpy(node->padding,(char *)wrapper_check + meta_size,sizeof(checking::value));
      node = store_->stores_[SAV]->Get(i);
      assert(sizeof(savings::value)  <= 16);
      memcpy(node->padding,(char *)wrapper_saving + meta_size,sizeof(savings::value));
#endif
      assert(node->seq == 2);

    }
    fprintf(stdout,"[Bank] partition %d total %lu loaded ,hot %lu\n",partition_,loaded_acct,
            loaded_hot);
    //check_remote_traverse();
  }

  void check_remote_traverse() {
    // check remote traverse, if possible
    char *rbuf = (char *)(cm->conn_buf_);

    int dev_id = cm->get_active_dev(0);
    int port_idx = cm->get_active_port(0);

    cm->thread_local_init();
    cm->open_device(dev_id);
    cm->register_connect_mr(dev_id); // register memory on the specific device

    cm->link_connect_qps(nthreads + nthreads + 1,dev_id,port_idx,0,IBV_QPT_RC);

    Qp *qp = cm->get_rc_qp(nthreads + nthreads + 1,current_partition,0);

    for(uint i = 0;i <= NumAccounts();++i) {

      uint64_t pid = AcctToPid(i);
      if(pid != current_partition) continue;

      // fetch
      MemNode *res_local = store_->stores_[CHECK]->Get(i);
      MemNode remote_node;
      uint64_t remote_off = store_->stores_[CHECK]->RemoteTraverse(i,qp,(char *)(&remote_node));
      assert((rbuf + remote_off) == (char *)res_local);
      assert(memcmp((char *)res_local,(char *)(&remote_node),sizeof(MemNode)) == 0);
    }
    fprintf(stdout,"check passed!\n");
  }
};


std::vector<BenchLoader *> BankMainRunner::make_loaders(int partition, MemDB* store) {
  std::vector<BenchLoader *> ret;
  if(store == NULL){
    ret.push_back(new BankLoader(9234,partition,store_,true));
  } else {
    ret.push_back(new BankLoader(9234,partition,store,false));
  }
  return ret;
}

std::vector<RWorker *> BankMainRunner::make_workers() {
  std::vector<RWorker *> ret;
  util::fast_random r(23984543 + current_partition * 73);

  for(uint i = 0;i < nthreads;++i) {
    ret.push_back(new BankWorker(i,r.next(),store_,ops_per_worker,&barrier_a_,&barrier_b_,this));
  }
#if SI_TX
  // add ts worker
  ts_manager = new TSManager(nthreads + nclients + 1,cm,0,0);
  ret.push_back(ts_manager);
#endif

#if CS == 1
  for(uint i = 0;i < nclients;++i)
    ret.push_back(new BankClient(nthreads + i,r.next()));
#endif
  return ret;
}

std::vector<BackupBenchWorker *> BankMainRunner::make_backup_workers() {
  std::vector<BackupBenchWorker *> ret;

  int num_backups = my_view->is_backup(current_partition);
  LogCleaner* log_cleaner = new BankLogCleaner;
  for(uint j = 0; j < num_backups; j++){
    assert(backup_stores_[j] != NULL);
    log_cleaner->add_backup_store(backup_stores_[j]);
  }

  DBLogger::set_log_cleaner(log_cleaner);
  for(uint i = 0; i < backup_nthreads; i++){
    ret.push_back(new BackupBenchWorker(i));
  }
  return ret;
}

void BankMainRunner::populate_cache() {


#if ONE_SIDED_READ == 1 && RDMA_CACHE == 1
  fprintf(stdout,"[Smallbank] Loading cache.\n");

  // create a temporal QP for usage
  int dev_id = cm->get_active_dev(0);
  int port_idx = cm->get_active_port(0);

  cm->thread_local_init();
  cm->open_device(dev_id);
  cm->register_connect_mr(dev_id); // register memory on the specific device

  cm->link_connect_qps(nthreads + nthreads + 1,dev_id,port_idx,0,IBV_QPT_RC);

  // calculate the time of populating the cache
  struct  timeval start;
  struct  timeval end;

  gettimeofday(&start,NULL);

  auto db = store_;
  char *temp = (char *)Rmalloc(256);

  for(uint64_t i = 0;i <= NumAccounts();++i) {

    auto pid = AcctToPid(i);

    auto off = db->stores_[CHECK]->RemoteTraverse(i,
                                                  cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
    assert(off != 0);

    off = db->stores_[SAV]->RemoteTraverse(i,
                                           cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
    assert(off != 0);
    Debugger::print_progress((double)i / NumAccounts());

  }
  Rfree(temp);
  gettimeofday(&end,NULL);

  auto diff = (end.tv_sec-start.tv_sec) + (end.tv_usec - start.tv_usec) /  1000000.0;
  fprintf(stdout,"Time to loader the caching is %f second\n",diff);
#endif
}


uint64_t NumAccounts(){
  return (uint64_t)(DEFAULT_NUM_ACCOUNTS * total_partition * scale_factor);
}
uint64_t NumHotAccounts(){
  return (uint64_t)(DEFAULT_NUM_HOT * total_partition * scale_factor);
}

uint64_t GetStartAcct() {
  return current_partition * DEFAULT_NUM_ACCOUNTS * scale_factor;
}

uint64_t GetEndAcct() {
  return (current_partition + 1) * DEFAULT_NUM_ACCOUNTS * scale_factor - 1;
}

}; // end namespace bank
}; // end banesspace oltp
} // end namespace nocc
