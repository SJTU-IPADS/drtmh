#pragma once

#include "tx_config.h"
#include "core/nocc_worker.h"
#include "core/commun_queue.hpp"
#include "core/utils/spinbarrier.h"

#include "util/util.h"

#include "db/txs/tx_handler.h"

#include <queue>          // std::queue

using namespace nocc::util;
using namespace nocc::db;

#define CS 0
#define LOCAL_CLIENT 0

namespace nocc {

  extern __thread coroutine_func_t *routines_;
  extern __thread TXHandler   **txs_;
  extern     RdmaCtrl *cm;

  extern __thread int *pending_counts_;

#define RPC_REQ 0

  namespace oltp {

    class BenchWorker;
    class BenchRunner;
    extern View* my_view; // replication setting of the data

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
      util::Breakdown_Timer latency_timer; // calculate the latency for each TX
      Profile p; // per tx profile
    };

    typedef std::vector<workload_desc> workload_desc_vec_t;

    extern __thread std::vector<size_t> *txn_counts;
    extern __thread std::vector<size_t> *txn_aborts;
    extern __thread std::vector<size_t> *txn_remote_counts;

    extern __thread workload_desc_vec_t *workloads;
    extern __thread bool init;

    /* Main benchmark worker */
    class BenchWorker : public Worker {
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
                  spin_barrier *barrier_a,spin_barrier *barrier_b,BenchRunner *context = NULL,
                  DBLogger *db_logger = NULL);

      void init_tx_ctx();

      virtual void exit_handler();
      virtual void run(); // run -> call worker routine
      virtual void worker_routine(yield_func_t &yield);

      void req_rpc_handler(int id,int cid,char *msg,void *arg);

      inline virtual void change_ctx(int cor_id) {
        tx_ = txs_[cor_id];
      }

      // simple wrapper to the underlying routine layer
      inline void context_transfer() {
        int next = routine_meta_->next_->id_;
        tx_      = txs_[next];
        cor_id_  = next;
        auto cur = routine_meta_;
        routine_meta_ = cur->next_;
      }

      inline void indirect_yield(yield_func_t& yield){
        // no pending request, no need for yield
        if(unlikely(pending_counts_[cor_id_] == 0 && !rpc_->has_pending_reqs(cor_id_)))
           return;
        int next = routine_meta_->next_->id_;
        tx_      = txs_[next];
        cor_id_  = next;
        auto cur = routine_meta_;
        routine_meta_ = cur->next_;
        cur->yield_from_routine_list(yield);
        assert(routine_meta_->id_ == cor_id_);
        change_ctx(cor_id_);
      }

      //tempraraily solve indirect_yield bug for logger
      inline void indirect_must_yield(yield_func_t& yield){
        int next = routine_meta_->next_->id_;
        tx_      = txs_[next];
        cor_id_  = next;
        auto cur = routine_meta_;
        routine_meta_ = cur->next_;
        cur->yield_from_routine_list(yield);
        assert(routine_meta_->id_ == cor_id_);
        change_ctx(cor_id_);
      }

      inline void yield_next(yield_func_t &yield) {
        // yield to the next routine
        int next = routine_meta_->next_->id_;
        // re-set next meta data
        tx_      = txs_[next];

        routine_meta_ = routine_meta_->next_;
        cor_id_  = next;
        routine_meta_->yield_to(yield);
      }

#if TX_USE_LOG
      void  init_logger() {
        assert(db_logger_ == NULL);
        assert(cm_ != NULL);
#if TX_LOG_STYLE == 0
        db_logger_ = new DBLogger(worker_id_,cm_,my_view,rdma_sched_);
#elif TX_LOG_STYLE == 1
        db_logger_ = new DBLogger(worker_id_,cm_,my_view,rpc_);
#elif TX_LOG_STYLE == 2
        db_logger_ = new DBLogger(worker_id_,cm_,my_view,rdma_sched_,rpc_);
#endif
        db_logger_->thread_local_init();
      };
#endif
      virtual ~BenchWorker() { }
      virtual workload_desc_vec_t get_workload() const = 0;
      virtual void register_callbacks() = 0;     /*register read-only callback*/
      virtual void check_consistency() {};
      virtual void thread_local_init() {};
      virtual void workload_report() { };

      /* we shall init msg_handler first, then init rpc_handler with msg_handler at the
         start of run time.
      */
      DBLogger *db_logger_;
      TXHandler *tx_;       /* current coroutine's tx handler */
      TXHandler *routine_1_tx_;

      LAT_VARS(yield);

      /* For statistics counts */
      size_t ntxn_commits_;
      size_t ntxn_aborts_;
      size_t ntxn_executed_;

      size_t ntxn_abort_ratio_;
      size_t ntxn_strict_counts_;
      size_t ntxn_remote_counts_;
      util::Breakdown_Timer latency_timer_;

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

    class BenchReporter { // not thread safe
    public:
      virtual void init(const std::vector<BenchWorker *> *workers);
      virtual void merge_data(char *);
      virtual void report_data(uint64_t epoch,std::ofstream &log_file);
      virtual void collect_data(char *data,struct timespec &start_t); // the data is stored in *data
      virtual size_t data_len();
      virtual void end();

    private:
      double throughput;
      double aborts;
      double abort_ratio;

      std::vector<uint64_t> prev_commits_;
      std::vector<uint64_t> prev_aborts_;
      std::vector<uint64_t> prev_abort_ratio_;

      const std::vector<BenchWorker *> *workers_;

      // helper functions
      uint64_t calculate_commits(std::vector<uint64_t> &prevs);
      uint64_t calculate_aborts(std::vector<uint64_t> &prevs);
      double   calculate_abort_ratio(std::vector<uint64_t> &prevs);
      double   calculate_execute_ratio();

    };

    class BenchLocalListener : public Worker {
    public:
      BenchLocalListener(unsigned worker_id,BenchReporter *reporter);
      virtual void run();
      virtual void worker_routine(yield_func_t &yield) { return;}
    private:
      BenchReporter *reporter_;
      void ending();
    };

    /* Bench listener is used to monitor system wide performance */
    class BenchListener : public ndb_thread {
    public:
      //RRpc *rpc_handler_;
      RRpc *rpc_;
      MsgHandler *msg_handler_;

      BenchListener(const std::vector<Worker *> *workers,BenchReporter *reporter);

      /* used to handler system exit */
      void sigint_handler(int);
      void ending();
      void run();

      double throughput;
      uint64_t aborts;
      uint64_t abort_ratio;

      int n_returned_;

      void get_result_rpc_handler(int id,int cid,char *msg,void *arg);
      void exit_rpc_handler(int id,int cid,char *msg,void *arg);
      void start_rpc_handler(int id,int cid,char *msg,void *arg);

    private:
      bool inited_; //whether all workers has inited
      BenchReporter *reporter_;
      std::vector<BenchWorker *> *workers_;
      uint64_t epoch_;

      void thread_local_init();
      void ending_record();
    };

    // used in a asymmetric setting
    class BenchClient : public Worker {
    public:
      BenchClient(unsigned worker_id,unsigned seed);
      virtual void run();
      virtual void worker_routine(yield_func_t &yield);
      void worker_routine_local(yield_func_t &yield);

      virtual int get_workload(char *input,fast_random &rand) = 0;
      virtual void exit_handler();

      Breakdown_Timer timer_;
    };

  }; // oltp
  extern __thread oltp::BenchWorker *worker;  // expose the worker
};   // nocc
