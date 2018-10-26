#include "tx_config.h"

#include "core/logging.h"
#include "rtx/occ.h"
#include "rtx/occ_rdma.h"

#include "bank_schema.h"
#include "bank_worker.h"

#include "framework/bench_runner.h"

#include "rdma_ctrl.hpp"
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
extern std::vector<std::string> cluster_topology;

namespace nocc {

extern RdmaCtrl *cm;       // global RDMA handler
namespace oltp {

extern char *store_buffer; // the buffer used to store DrTM-kv
extern MemDB *backup_stores_[MAX_BACKUP_NUM];

extern char *rdma_buffer;
extern uint64_t r_buffer_size;

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

  int meta_size = META_LENGTH;
#if ONE_SIDED_READ
  meta_size = sizeof(rtx::RdmaValHeader);
#endif

  //store->AddSchema(ACCT, TAB_HASH,sizeof(uint64_t),sizeof(account::value),meta_size);
  store->AddSchema(SAV,  TAB_HASH,sizeof(uint64_t),sizeof(savings::value),meta_size,
                   NumAccounts() / total_partition * 1.5);
  store->AddSchema(CHECK,TAB_HASH,sizeof(uint64_t),sizeof(checking::value),meta_size,
                   NumAccounts() / total_partition * 1.5);

#if ONE_SIDED_READ == 1
  //store->EnableRemoteAccess(ACCT,cm);
  store->EnableRemoteAccess(SAV,rdma_buffer);
  store->EnableRemoteAccess(CHECK,rdma_buffer);
#endif
}

void BankMainRunner::init_backup_store(MemDB* &store){
  assert(store == NULL);
  store = new MemDB();
  int meta_size = META_LENGTH;

#if ONE_SIDED_READ
  meta_size = sizeof(rtx::RdmaValHeader);
#endif

  store->AddSchema(SAV,  TAB_HASH,sizeof(uint64_t),sizeof(savings::value),meta_size,
                   NumAccounts() / total_partition,false);
  store->AddSchema(CHECK,TAB_HASH,sizeof(uint64_t),sizeof(checking::value),meta_size,
                   NumAccounts() / total_partition,false);
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

#if ONE_SIDED_READ == 1
    if(is_primary_)
      RThreadLocalInit();
#endif

    fprintf(stdout,"[Bank], total %lu accounts loaded\n", NumAccounts());
    int meta_size = store_->_schemas[CHECK].meta_len;

    char acct_name[32];
    const char *acctNameFormat = "%lld 32 d";

    uint64_t loaded_acct(0),loaded_hot(0);

    for(uint64_t i = 0;i <= NumAccounts();++i){

      uint64_t pid = AcctToPid(i);
      assert(0 <= pid && pid < total_partition);
      if(pid != partition_) continue;

      uint64_t round_sz = CACHE_LINE_SZ << 1; // 128 = 2 * cacheline to avoid false sharing

      char *wrapper_acct(NULL), *wrapper_saving(NULL), *wrapper_check(NULL);
      int save_size = meta_size + sizeof(savings::value);
      save_size = Round<int>(save_size,sizeof(uint64_t));
      int check_size = meta_size + sizeof(checking::value);
      check_size = Round<int>(check_size, sizeof(uint64_t));
      ASSERT(check_size % sizeof(uint64_t) == 0) << "cache size " << check_size;

#if ONE_SIDED_READ == 1
      if(is_primary_){
        wrapper_saving = (char *)Rmalloc(save_size);
        wrapper_check  = (char *)Rmalloc(check_size);
      } else {
        wrapper_saving = new char[save_size];
        wrapper_check  = new char[check_size];
      }
#else
      wrapper_saving = new char[save_size];
      wrapper_check  = new char[check_size];
#endif
      wrapper_acct = new char[meta_size + sizeof(account::value)];
      assert(wrapper_saving != NULL);
      assert(wrapper_check != NULL);
      assert(wrapper_acct != NULL);

      loaded_acct += 1;

      if(i < NumHotAccounts())
        loaded_hot += 1;

      sprintf(acct_name,acctNameFormat,i);

      memset(wrapper_acct, 0, meta_size);
      memset(wrapper_saving, 0, meta_size);
      memset(wrapper_check,  0, meta_size);

      account::value *a = (account::value*)(wrapper_acct + meta_size);
      a->a_name.assign(std::string(acct_name) );

      float balance_c = (float)_RandomNumber(random_generator_, MIN_BALANCE, MAX_BALANCE);
      float balance_s = (float)_RandomNumber(random_generator_, MIN_BALANCE, MAX_BALANCE);

      savings::value *s = (savings::value *)(wrapper_saving + meta_size);
      s->s_balance = balance_s;
      auto node = store_->Put(SAV,i,(uint64_t *)wrapper_saving,sizeof(savings::value));
      if(is_primary_ && ONE_SIDED_READ) {
        node->off = (uint64_t)wrapper_saving - (uint64_t)(rdma_buffer);
        ASSERT(node->off % sizeof(uint64_t) == 0) << "saving value size " << save_size;
      }

      checking::value *c = (checking::value *)(wrapper_check + meta_size);
      c->c_balance = balance_c;
      assert(c->c_balance > 0);
      node = store_->Put(CHECK,i,(uint64_t *)wrapper_check,sizeof(checking::value));

      if(i == 0)
        LOG(3) << "check cv balance " << c->c_balance;

      if(is_primary_ && ONE_SIDED_READ) {
        node->off =  (uint64_t)wrapper_check - (uint64_t)(rdma_buffer);
        ASSERT(node->off % sizeof(uint64_t) == 0) << "check value size " << check_size;
      }

      assert(node->seq == 2);

    }
    //check_remote_traverse();
  }

  void check_remote_traverse() {
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

void BankMainRunner::populate_cache() {

#if ONE_SIDED_READ == 1 && RDMA_CACHE == 1

  LOG(2) << "loading smallbank's cache.";

  // calculate the time of populating the cache
  struct  timeval start;
  struct  timeval end;

  gettimeofday(&start,NULL);

  auto db = store_;
  char *temp = (char *)Rmalloc(256);

  int wid = nthreads + nthreads + 1;
  RdmaCtrl::DevIdx nic_idx = cm->convert_port_idx(0);  // use the zero nic
  ASSERT(cm->open_device(nic_idx) != nullptr);
  ASSERT(cm->register_memory(wid,rdma_buffer,r_buffer_size,cm->get_device()) == true);
  cm->link_symmetric_rcqps(cluster_topology,
                            wid, // local mr_id
                            wid, // mr_id
                            wid  /* as thread id */);

  for(uint64_t i = 0;i <= NumAccounts();++i) {

    auto pid = AcctToPid(i);

    auto off = db->stores_[CHECK]->RemoteTraverse(i,cm->get_rc_qp(create_rc_idx(pid,wid)),temp);
    assert(off != 0);

    off = db->stores_[SAV]->RemoteTraverse(i,cm->get_rc_qp(create_rc_idx(pid,wid)),temp);
    assert(off != 0);

    if(i % (total_partition * 10000) == 0)
      PrintProgress((double)i / NumAccounts());

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
