#include "rocc_config.h"
#include "tx_config.h"

#include "config.h"
#include "global_config.h"

#include "bench_worker.hpp"

#include "req_buf_allocator.h"

extern size_t coroutine_num;
extern size_t current_partition;
extern size_t nclients;
extern size_t nthreads;
extern int tcp_port;

namespace nocc {

  __thread oltp::BenchWorker *worker = NULL;
  __thread TXHandler   **txs_ = NULL;

  extern uint64_t total_ring_sz;
  extern uint64_t ring_padding;

  std::vector<CommQueue *> conns; // connections between clients and servers
  extern std::vector<SingleQueue *>   local_comm_queues;
  extern zmq::context_t send_context;

  namespace oltp {

    // used to calculate benchmark information ////////////////////////////////
    __thread std::vector<size_t> *txn_counts = NULL;
    __thread std::vector<size_t> *txn_aborts = NULL;
    __thread std::vector<size_t> *txn_remote_counts = NULL;

    // used to calculate the latency of each workloads
    __thread workload_desc_vec_t *workloads;

    extern char *rdma_buffer;

    extern __thread util::fast_random *random_generator;

    // per-thread memory allocator
    __thread RPCMemAllocator *msg_buf_alloctors = NULL;

    SpinLock exit_lock;

    BenchWorker::BenchWorker(unsigned worker_id,bool set_core,unsigned seed,uint64_t total_ops,
                             spin_barrier *barrier_a,spin_barrier *barrier_b,BenchRunner *context,
                             DBLogger *db_logger):
      Worker(worker_id,cm,seed),
      initilized_(false),
      set_core_id_(set_core),
      ntxn_commits_(0),
      ntxn_aborts_(0),
      ntxn_executed_(0),
      ntxn_abort_ratio_(0),
      ntxn_remote_counts_(0),
      ntxn_strict_counts_(0),
      total_ops_(total_ops),
      context_(context),
      db_logger_(db_logger),
      // r-set some local members
      routine_1_tx_(NULL) {
      assert(cm_ != NULL);
      INIT_LAT_VARS(yield);
#if CS == 0
      nclients = 0;
      server_routine = coroutine_num;
#else
      if(nclients >= nthreads) server_routine = coroutine_num;
      else server_routine = MAX(coroutine_num,server_routine);
      //server_routine = MAX(40,server_routine);
#endif
      //server_routine = 20;
    }

    void BenchWorker::init_tx_ctx() {

      worker = this;
      txs_              = new TXHandler*[1 + server_routine + 2];
      msg_buf_alloctors = new RPCMemAllocator[1 + server_routine];

      txn_counts = new std::vector<size_t> ();
      txn_aborts = new std::vector<size_t> ();
      txn_remote_counts = new std::vector<size_t> ();

      for(uint i = 0;i < NOCC_BENCH_MAX_TX;++i) {
        txn_counts->push_back(0);
        txn_aborts->push_back(0);
        txn_remote_counts->push_back(0);
      }

      // init workloads
      workloads = new workload_desc_vec_t[server_routine + 2];
    }

    void BenchWorker::run() {

      // create connections
      exit_lock.Lock();
      if(conns.size() == 0) {
        // only create once
        for(uint i = 0;i < nthreads + nclients + 1;++i)
          conns.push_back(new CommQueue(nthreads + nclients + 1));
      }
      exit_lock.Unlock();

      fprintf(stdout,"[worker %d] started with %d server routines\n",worker_id_,server_routine);
      BindToCore(worker_id_); // really specified to platforms
      init_tx_ctx();
      init_routines(server_routine);

#if USE_RDMA
      init_rdma();
      create_qps();
#endif

#if USE_TCP_MSG == 1
      assert(local_comm_queues.size() > 0);
      create_tcp_connections(local_comm_queues[worker_id_],tcp_port,send_context);
#else
      MSGER_TYPE type;

#if USE_UD_MSG == 1
      type = UD_MSG;
#if LOCAL_CLIENT == 1 || CS == 0
      int total_connections = 1;
#else
      int total_connections = nthreads + nclients;
#endif
      create_rdma_ud_connections(total_connections);
#else
      create_rdma_rc_connections(rdma_buffer + HUGE_PAGE_SZ,
                                 total_ring_sz,ring_padding);
#endif

#endif

#if TX_USE_LOG
      this->init_logger();
#endif
      this->thread_local_init();   // application specific init
      register_callbacks();
#if CS == 1
#if LOCAL_CLIENT == 0
      create_client_connections(nthreads + nclients);
#endif
      assert(pending_reqs_.empty());
      rpc_->register_callback(std::bind(&BenchWorker::req_rpc_handler,this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        std::placeholders::_3,
                                        std::placeholders::_4),RPC_REQ);
#endif

      // waiting for master to start workers
      this->inited = true;
#if 0
      while(!this->running) {
        asm volatile("" ::: "memory");
      }
#else
      this->running = true;
#endif
      // starts the new_master_routine
      start_routine(); // uses parent worker->start
    }

    void __attribute__((optimize("O1"))) // this flag is very tricky, it should be set this way
    BenchWorker::worker_routine(yield_func_t &yield) {

      assert(conns.size() != 0);

      using namespace db;
      /* worker routine that is used to run transactions */
      workloads[cor_id_] = get_workload();
      auto &workload = workloads[cor_id_];

      // Used for OCC retry
      unsigned int backoff_shifts = 0;
      unsigned long abort_seed = 73;

      while(abort_seed == random_generator[cor_id_].get_seed()) {
        abort_seed += 1;         // avoids seed collision
      }

#if SI_TX
      if(current_partition == 0) indirect_must_yield(yield);
#endif


      uint64_t retry_count(0);
      while(true) {
#if CS == 0
        /* select the workload */
        double d = random_generator[cor_id_].next_uniform();

        uint tx_idx = 0;
        for(size_t i = 0;i < workload.size();++i) {
          if((i + 1) == workload.size() || d < workload[i].frequency) {
            tx_idx = i;
            break;
          }
          d -= workload[i].frequency;
        }
#else
#if LOCAL_CLIENT
        REQ req;
        if(!conns[worker_id_]->front((char *)(&req))){
          yield_next(yield);
          continue;
        }
        conns[worker_id_]->pop();
#else
        if(pending_reqs_.empty()){
          yield_next(yield);
          continue;
        }
        REQ req = pending_reqs_.front();
        pending_reqs_.pop();
#endif
        int tx_idx = req.tx_id;
#endif

#if CALCULATE_LAT == 1
        if(cor_id_ == 1) {
          // only profile the latency for cor 1
          //#if LATENCY == 1
          latency_timer_.start();
          //#else
          (workload[tx_idx].latency_timer).start();
          //#endif
        }
#endif
        const unsigned long old_seed = random_generator[cor_id_].get_seed();
        ntxn_executed_ += 1;
        (*txn_counts)[tx_idx] += 1;
      abort_retry:
        auto ret = workload[tx_idx].fn(this,yield);
#if NO_ABORT == 1
        ret.first = true;
#endif
        // if(current_partition == 0){
        if(likely(ret.first)) {
          // commit case
          retry_count = 0;
#if CALCULATE_LAT == 1
          if(cor_id_ == 1) {
            //#if LATENCY == 1
            latency_timer_.end();
            //#else
            workload[tx_idx].latency_timer.end();
            //#endif
          }
#endif

#if CS == 0 // calculate results there
          ntxn_commits_ += 1;

#if PROFILE_RW_SET == 1 || PROFILE_SERVER_NUM == 1
          if(ret.second > 0)
            workload[tx_idx].p.process_rw(ret.second);
#endif
#else
          ntxn_commits_ += 1;
          // reply to client
#if LOCAL_CLIENT
          char dummy = req.cor_id;
          conns[req.c_tid]->enqueue(worker_id_,(char *)(&dummy),sizeof(char));
#else
          char *reply = rpc_->get_reply_buf();
          rpc_->send_reply(reply,sizeof(uint8_t),req.c_id,req.c_tid,req.cor_id,client_handler_);
#endif
#endif
        } else {
          retry_count += 1;
          //if(retry_count > 10000000) assert(false);
          // abort case
          if(old_seed != abort_seed) {
            /* avoid too much calculation */
            ntxn_abort_ratio_ += 1;
            abort_seed = old_seed;
            (*txn_aborts)[tx_idx] += 1;
          }
          ntxn_aborts_ += 1;
          yield_next(yield);

          // reset the old seed
          random_generator[cor_id_].set_seed(old_seed);
          goto abort_retry;
        }
        yield_next(yield);
        // end worker main loop
      }
    }

    void BenchWorker::exit_handler() {

      if( worker_id_ == 0 ){

        // only sample a few worker information
        auto &workload = workloads[1];

        auto second_cycle = Breakdown_Timer::get_one_second_cycle();

        //exit_lock.Lock();
        fprintf(stdout,"aborts: ");
        workload[0].latency_timer.calculate_detailed();
        fprintf(stdout,"%s ratio: %f ,executed %lu, latency %f, rw_size %f, m %f, 90 %f, 99 %f\n",
                workload[0].name.c_str(),
                (double)((*txn_aborts)[0]) / ((*txn_counts)[0] + ((*txn_counts)[0] == 0)),
                (*txn_counts)[0],workload[0].latency_timer.report() / second_cycle * 1000,
                workload[0].p.report(),
                workload[0].latency_timer.report_medium() / second_cycle * 1000,
                workload[0].latency_timer.report_90() / second_cycle * 1000,
                workload[0].latency_timer.report_99() / second_cycle * 1000);

        for(uint i = 1;i < workload.size();++i) {
          workload[i].latency_timer.calculate_detailed();
          fprintf(stdout,"        %s ratio: %f ,executed %lu, latency: %f, rw_size %f, m %f, 90 %f, 99 %f\n",
                  workload[i].name.c_str(),
                  (double)((*txn_aborts)[i]) / ((*txn_counts)[i] + ((*txn_counts)[i] == 0)),
                  (*txn_counts)[i],
                  workload[i].latency_timer.report() / second_cycle * 1000,
                  workload[i].p.report(),
                  workload[i].latency_timer.report_medium() / second_cycle * 1000,
                  workload[i].latency_timer.report_90() / second_cycle * 1000,
                  workload[i].latency_timer.report_99() / second_cycle * 1000);
        }
        fprintf(stdout,"\n");

        fprintf(stdout,"total: ");
        for(uint i = 0;i < workload.size();++i) {
          fprintf(stdout," %d %lu; ",i, (*txn_counts)[i]);
        }
        fprintf(stdout,"succs ratio %f\n",(double)(ntxn_executed_) /
                (double)(ntxn_commits_));

        check_consistency();

        //exit_lock.Unlock();

        fprintf(stdout,"master routine exit...\n");
      }
      return;
    }

    /* Abstract bench loader */
    BenchLoader::BenchLoader(unsigned long seed)
      : random_generator_(seed) {
      worker_id_ = 0; /**/
    }

    void BenchLoader::run() {
      load();
    }

    BenchClient::BenchClient(unsigned worker_id,unsigned seed)
      :Worker(worker_id,cm,seed) { }

    void BenchClient::run() {

      fprintf(stdout,"[Client %d] started\n",worker_id_);
      // client only support ud msg for communication
      BindToCore(worker_id_);
#if USE_RDMA
      init_rdma();
#endif
      init_routines(coroutine_num);
      //create_server_connections(UD_MSG,nthreads);
#if CS == 1 && LOCAL_CLIENT == 0
      create_client_connections();
#endif
      running = true;
      start_routine();
    }

    void BenchClient::worker_routine_local(yield_func_t &yield) {

      uint64_t *start = new uint64_t[coroutine_num];
      for(uint i = 0;i < coroutine_num;++i) {
        BenchWorker::REQ req;
        uint8_t tx_id;
        auto node = get_workload((char *)(&tx_id),random_generator[i]);
        req.c_tid = worker_id_;
        req.tx_id = tx_id;
        req.cor_id = i;
        uint thread = random_generator[i].next() % nthreads;
        start[i] = rdtsc();
        conns[thread]->enqueue(worker_id_,(char *)(&req),sizeof(BenchWorker::REQ));
      }
      
      while(running) {
        char res[16];
        if(conns[worker_id_]->front(res)) {
          int cid = (int)res[0];
          conns[worker_id_]->pop();
          auto latency = rdtsc() - start[cid];
          timer_.emplace(latency);

          BenchWorker::REQ req;
          uint8_t tx_id;
          auto node = get_workload((char *)(&tx_id),random_generator[cid]);
          req.c_tid = worker_id_;
          req.tx_id = tx_id;
          req.cor_id = cid;
          uint thread = random_generator[cid].next() % nthreads;
          start[cid] = rdtsc();
          conns[thread]->enqueue(worker_id_,(char *)(&req),sizeof(BenchWorker::REQ));
        }
        // end while
      }
    }

    void BenchClient::worker_routine(yield_func_t &yield) {
#if LOCAL_CLIENT
      return worker_routine_local(yield);
#endif

      char *req_buf = rpc_->get_static_buf(64);
      char reply_buf[64];
      while(true) {
        auto start = rdtsc();

        // prepare arg
        char *arg = (char *)req_buf;
        auto node = get_workload(arg + sizeof(uint8_t),random_generator[cor_id_]);
        *((uint8_t *)(arg)) = worker_id_;
        uint thread = random_generator[cor_id_].next() % nthreads;

        // send to server
        rpc_->prepare_multi_req(reply_buf,1,cor_id_);
        rpc_->append_req(req_buf,RPC_REQ,sizeof(uint64_t),cor_id_,RRpc::REQ,node,thread);

        // yield
        default_yield(yield);
        // get results back
        auto latency = rdtsc() - start;
        timer_.emplace(latency);
      }
    }

    void BenchClient::exit_handler() {
#if LOCAL_CLIENT == 0
      auto second_cycle = util::Breakdown_Timer::get_one_second_cycle();
      auto m_av = timer_.report_avg() / second_cycle * 1000;
      //exit_lock.Lock();
      fprintf(stdout,"avg latency for client %d, %3f ms\n",worker_id_,m_av);
      //exit_lock.Unlock();
#endif
    }

    void BenchWorker::req_rpc_handler(int id,int cid,char *msg,void *arg) {
      uint8_t *input = (uint8_t *)msg;
      pending_reqs_.emplace(input[1],id,input[0],cid);
    }



  }; // oltp

}; // nocc
