#pragma once

#include "tx_config.h"
#include "core/rworker.h"
#include "core/commun_queue.hpp"
#include "core/utils/spinbarrier.h"
#include "core/utils/count_vector.hpp"

#include "util/util.h"

#include "rtx/logger.hpp"
#include "rtx/occ.h"

#include <queue>          // std::queue

using namespace nocc::util;

#define CS 0
#define LOCAL_CLIENT 0

extern size_t current_partition;
extern size_t total_partition;
extern size_t nthreads;
extern size_t coroutine_num;

namespace nocc {

extern __thread coroutine_func_t *routines_;
extern __thread rtx::OCC    **new_txs_;
extern     RdmaCtrl *cm;

extern __thread int *pending_counts_;

#define RPC_REQ 0

namespace oltp {

class BenchWorker;
class BenchRunner;

extern char *rdma_buffer;

/* Txn result return type */
typedef std::pair<bool, double> txn_result_t;

/* Registerered Txn execution function */
typedef txn_result_t (*txn_fn_t)(BenchWorker *,yield_func_t &yield);
struct workload_desc {
  workload_desc() {}
  workload_desc(const std::string &name, double frequency, txn_fn_t fn)
      : name(name), frequency(frequency), fn(fn)
  {
    ALWAYS_ASSERT(frequency >= 0.0);
    ALWAYS_ASSERT(frequency <= 1.0);
  }
  std::string name;
  double frequency;
  txn_fn_t fn;
  util::BreakdownTimer latency_timer; // calculate the latency for each TX
};

typedef std::vector<workload_desc> workload_desc_vec_t;

extern __thread std::vector<size_t> *txn_counts;
extern __thread std::vector<size_t> *txn_aborts;
extern __thread std::vector<size_t> *txn_remote_counts;

extern __thread workload_desc_vec_t *workloads;
extern __thread bool init;

/* Main benchmark worker */
class BenchWorker : public RWorker {
 public:

  // used for CS request
  struct REQ {
    int tx_id;
    int c_id;
    int c_tid;
    int cor_id;
    REQ(int t,int cid,int ctid,int cor) : tx_id(t),c_id(cid),c_tid(ctid),cor_id(cor) {}
    REQ() { }
  };

  int server_routine = 10;
  uint64_t total_ops_;

  /* methods */
  BenchWorker(unsigned worker_id,bool set_core,unsigned seed,uint64_t total_ops,
              spin_barrier *barrier_a,spin_barrier *barrier_b,BenchRunner *context = NULL);

  void init_tx_ctx();

  virtual void exit_handler();
  virtual void run(); // run -> call worker routine
  virtual void worker_routine(yield_func_t &yield);

  void req_rpc_handler(int id,int cid,char *msg,void *arg);

  void change_ctx(int cor_id) {
    rtx_ = new_txs_[cor_id];
  }

  // simple wrapper to the underlying routine layer
  inline void context_transfer() {
    int next = routine_meta_->next_->id_;
    cor_id_  = next;
    auto cur = routine_meta_;
    routine_meta_ = cur->next_;
  }

  void init_new_logger(MemDB **backup_stores) {

    assert(new_logger_ == NULL);
    uint64_t M2 = HUGE_PAGE_SZ;

#if TX_LOG_STYLE == 1 // RPC's case
    if(worker_id_ == 0)
      LOG(3) << "Use RPC for logging.";
    new_logger_ = new rtx::RpcLogger(rpc_,RTX_LOG_RPC_ID,RTX_LOG_CLEAN_ID,M2,
                                     MAX_BACKUP_NUM,rdma_buffer + HUGE_PAGE_SZ,
                                     total_partition,nthreads,(coroutine_num + 1) * RTX_LOG_ENTRY_SIZE);
#elif TX_LOG_STYLE == 2 // one-sided RDMA case
    if(worker_id_ == 0)
      LOG(3) << "Use RDMA for logging.";
    new_logger_ = new rtx::RDMALogger(cm_,rdma_sched_,current_partition,worker_id_,M2,
                                      rpc_,RTX_LOG_CLEAN_ID,
                                      MAX_BACKUP_NUM,rdma_buffer + HUGE_PAGE_SZ,
                                      total_partition,nthreads,(coroutine_num) * RTX_LOG_ENTRY_SIZE);
#endif

    // add backup stores)
    if(new_logger_) {
      std::set<int> backups;
      int num_backups = rtx::global_view->response_for(current_partition,backups);
      ASSERT(num_backups <= MAX_BACKUP_NUM)
          << "I'm backed for " << num_backups << "s!"
          << "Max " << MAX_BACKUP_NUM << " supported.";

      int i(0);
      for(auto it = backups.begin();it != backups.end();++it) {
        int backed_id = *it;
        assert(backed_id != current_partition);
        new_logger_->add_backup_store(backed_id,backup_stores[i++]);
      }
    }
  }

  virtual ~BenchWorker() { }
  virtual workload_desc_vec_t get_workload() const = 0;
  virtual void register_callbacks() = 0;     /*register read-only callback*/
  virtual void check_consistency() {};
  virtual void thread_local_init() {};
  virtual void workload_report() { };
  virtual void exit_report() { };

  /* we shall init msg_handler first, then init rpc_handler with msg_handler at the
     start of run time.
  */
  rtx::Logger *new_logger_;

  rtx::OCC *rtx_;
  rtx::OCC *rtx_hook_ = NULL;

  LAT_VARS(yield);

  /* For statistics counts */
  size_t ntxn_commits_;
  size_t ntxn_aborts_;
  size_t ntxn_executed_;

  size_t ntxn_abort_ratio_;
  size_t ntxn_strict_counts_;
  size_t ntxn_remote_counts_;
  util::BreakdownTimer latency_timer_;
  CountVector<double> latencys_;

 private:
  bool initilized_;
  /* Does bind the core */
  bool set_core_id_;
  spin_barrier *barrier_a_;
  spin_barrier *barrier_b_;
  BenchRunner  *context_;

  std::queue<REQ> pending_reqs_;
};

class BenchLoader : public ndb_thread {
 public:
  BenchLoader(unsigned long seed) ;
  void run();
 protected:
  virtual void load() = 0;
  util::fast_random random_generator_;
  int partition_;
 private:
  unsigned int worker_id_;
};


// used in a asymmetric setting
class BenchClient : public RWorker {
 public:
  BenchClient(unsigned worker_id,unsigned seed);
  virtual void run();
  virtual void worker_routine(yield_func_t &yield);
  void worker_routine_local(yield_func_t &yield);

  virtual int get_workload(char *input,fast_random &rand) = 0;
  virtual void exit_handler();

  BreakdownTimer timer_;
};

}; // oltp
extern __thread oltp::BenchWorker *worker;  // expose the worker
};   // nocc
