#include "rocc_config.h"

#include "bench_worker.hpp"

#include "core/nocc_worker.h"
#include "core/routine.h"
#include "core/tcp_adapter.hpp"

// RDMA related
#include "ralloc.h"
#include "ring_msg.h"
#include "ud_msg.h"

#include "util/util.h"
#include "util/printer.h"

#include <signal.h>
#include <vector>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>

#include <pthread.h>
#include <sched.h>


/* bench listener is used to monitor the various performance field of the current system */

/* global config constants */
extern size_t nthreads;
extern size_t nclients;
extern size_t current_partition;
extern size_t total_partition;
extern size_t coroutine_num;
extern size_t distributed_ratio;

extern std::string exe_name;
extern std::string bench_type;

#define RPC_REPORT 1
#define RPC_EXIT   2
#define RPC_START  3

#ifdef LOG_RESULTS
#include <fstream>
std::ofstream log_file;
#endif


using namespace rdmaio;
using namespace rdmaio::ringmsg;
using namespace nocc::util;

#undef  USE_UD_MSG
#define USE_UD_MSG  1

extern int tcp_port;

namespace nocc {

  extern volatile bool running;
  extern RdmaCtrl *cm;
  extern __thread TXHandler **txs_;
  extern std::vector<SingleQueue *>   local_comm_queues;
  extern zmq::context_t send_context;

  namespace oltp {

    extern uint64_t total_ring_sz;
    extern uint64_t ring_padding;
    extern uint64_t ringsz;

    extern char *rdma_buffer;

    int worker_id;

    static struct timespec start_t;

    struct listener_ping_result  {
      double throughput;
      int32_t aborts;
      int32_t abort_ratio;
    };

    boost::function<void(int)> sigint_callback;
    void sigint_callback_wrapper(int value)
    {
      sigint_callback(value);
    }

    // comment ////////////////////////////////////////////////////////////////

    BenchListener::BenchListener(const std::vector<Worker *> *workers,BenchReporter *reporter)
      :epoch_(0),
       inited_(false),
       reporter_(reporter),
       n_returned_(0)
    {
      // init worker
      workers_ = new std::vector<BenchWorker *>();
      assert(workers->size() >= nthreads);
      for(uint i = 0;i < workers->size();++i) {
        workers_->push_back(static_cast<BenchWorker *>((*workers)[i]));
      }

      assert(cm != NULL);
      worker_id = nthreads + nclients + 2;

      /* register sigint handler */
      if(current_partition == 0) {
        sigint_callback = std::bind1st(std::mem_fun(&BenchListener::sigint_handler),this);
        signal(SIGINT,sigint_callback_wrapper);
      }

#ifdef LOG_RESULTS
      if(current_partition == 0) {
        char log_file_name[64];
        snprintf(log_file_name,64,"./results/%s_%s_%lu_%lu_%lu_%lu.log",
                 exe_name.c_str(),bench_type.c_str(),total_partition,nthreads,coroutine_num,distributed_ratio);
        Debugger::debug_fprintf(stdout,"log to %s\n",log_file_name);
        log_file.open(log_file_name,std::ofstream::out);
        if(!log_file.is_open()) {
          // create the directory if necessary

        }
        assert(log_file.is_open());
      }
#else
      //      assert(false);
#endif

      reporter->init(workers_);
    }

    void BenchListener::thread_local_init() {

      // create qps and init msg handlers
      int dev_id = 0;
      int port_idx = 1;
#if USE_RDMA
      cm->thread_local_init();
      cm->open_device(dev_id);
      cm->register_connect_mr(dev_id); // register memory on the specific device
#endif
      using namespace rdmaio::udmsg;

      //rpc_handler_ = new Rpc(NULL,worker_id);
      rpc_ = new RRpc(worker_id,1,1,1);
#if USE_RDMA == 1
      msg_handler_ = new UDMsg(cm,worker_id,1,
                               MAX_SERVER_TO_SENT,std::bind(&RRpc::poll_comp_callback,rpc_,
                                                            std::placeholders::_1,
                                                            std::placeholders::_2,
                                                            std::placeholders::_3),
                               dev_id,port_idx,1);
#else
      assert(worker_id < local_comm_queues.size());
      auto adapter = new Adapter(std::bind(&Rpc::poll_comp_callback,rpc_handler_,
                                           std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
                                 worker_id,local_comm_queues[worker_id]);
#if DEDICATED
      adapter->create_dedicated_sockets(cm->network_,tcp_port,send_context);
#endif
      msg_handler_ = adapter;
#endif

      //rpc_handler_->message_handler_ = msg_handler_; // reset the msg handler
      rpc_->set_msg_handler(msg_handler_);

      rpc_->register_callback(std::bind(&BenchListener::get_result_rpc_handler,this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        std::placeholders::_3,
                                        std::placeholders::_4),RPC_REPORT);
      rpc_->register_callback(std::bind(&BenchListener::exit_rpc_handler,this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        std::placeholders::_3,
                                        std::placeholders::_4),RPC_EXIT);
      rpc_->register_callback(std::bind(&BenchListener::start_rpc_handler,this,
                                        std::placeholders::_1,
                                        std::placeholders::_2,
                                        std::placeholders::_3,
                                        std::placeholders::_4),RPC_START);

      // init routines
      //RoutineMeta::thread_local_init();
    }

    void BenchListener::run() {
#if 0
      pthread_t this_thread = pthread_self();
      struct sched_param params;
      // We'll set the priority to the maximum.
      params.sched_priority = sched_get_priority_max(SCHED_FIFO);
      auto ret = pthread_setschedparam(this_thread, SCHED_FIFO, &params);
      assert(ret == 0);
#endif

      fprintf(stdout,"[Listener]: Monitor running!\n");
      sleep(1);
      // add a bind?
      //util::BindToCore(nthreads);
      /* Some prepartion stuff */
      RThreadLocalInit();
      thread_local_init();

      // wait till all workers is running before start the worker
      int done = 0;
    WAIT_RETRY:
      for(uint i = 0;i < workers_->size();++i) {
        if( (*workers_)[i]->init_status())
          done += 1;
      }
      if(done != workers_->size()) {
        done = 0;
        goto WAIT_RETRY;
      }

      if(current_partition == 0) {
        /* the first server is used to report results */
        //char *msg_buf = (char *)Rmalloc(1024);
        //char *payload_ptr = msg_buf + sizeof(uint64_t) + sizeof(rpc_header);
        char *msg_buf = rpc_->get_static_buf(1024);

        fprintf(stdout,"[Listener]: Enter main loop\n");

        try {
          while(true) {

            if(unlikely(running == false)) {

              fprintf(stdout,"[Listener] receive ending..\n");
              int *node_ids = new int[msg_handler_->get_num_nodes()];
              for(uint i = 1;i < msg_handler_->get_num_nodes();++i) {
                node_ids[i-1] = i;
              }
              /* so that the send requests shall poll completion */
              //msg_handler_->force_sync(node_ids,msg_handler_->get_num_nodes() - 1);
              if(msg_handler_->get_num_nodes() > 1) {
                //rpc_handler_->send_reqs(RPC_EXIT,sizeof(uint64_t),node_ids,msg_handler_->get_num_nodes() - 1,0);
                rpc_->broadcast_to(msg_buf,
                                   RPC_EXIT,sizeof(uint64_t),1,RRpc::REQ,
                                   node_ids,msg_handler_->get_num_nodes() - 1);
              }
              ending_record();
              // caluclate local latency
              ending();
              /* shall never return... */
            }

            if(msg_handler_->get_num_nodes() == 1) {
              /* Single server special case */
              sleep(1);
              get_result_rpc_handler(0,0,NULL,NULL);
            }
            msg_handler_->poll_comps();
#if 1
            if(!inited_) {
              // send start rpc to others
              for(uint i = 1;i < msg_handler_->get_num_nodes();++i) {
                //rpc_handler_->append_req_ud(payload_ptr,RPC_START,sizeof(uint64_t),i,0);
                //rpc_handler_->append_req(payload_ptr,RPC_START,sizeof(uint64_t),i,worker_id,0);
                rpc_->append_req(msg_buf,RPC_START,sizeof(uint64_t),0,RRpc::REQ,i);
              }
              fprintf(stdout,"sent started\n");
              // for me to start
              start_rpc_handler(0,0,NULL,NULL);
            }
#endif
            /* end monitoring */
            if(epoch_ >= MASTER_EPOCH) {
              /* exit */
              fprintf(stdout,"[Listener] Master exit\n");
              //ending();
              //exit_rpc_handler(0,NULL,NULL);
              for(auto it = workers_->begin();it != workers_->end();++it) {
                (*it)->end_routine();
              }
              sleep(1);

              running = false;
            }
          } }
        catch (...) {
          assert(false);
        }
      } else {

        // other server's case
        char *msg_buf = rpc_->get_static_buf(64);
        while(true) {
          /* report results one time one second */
          msg_handler_->poll_comps();
          if(inited_) {
            sleep(1);
          } else
            continue; // receive start RPC
#if 1
          /* count the throughput of current server*/
          reporter_->collect_data(msg_buf,start_t);

          /* master server's id is 0 */
          int master_id = 0;
          //rpc_handler_->send_reqs(RPC_REPORT,sizeof(struct listener_ping_result),&master_id,1,0);
          rpc_->append_req(msg_buf,RPC_REPORT,sizeof(listener_ping_result),0,RRpc::REQ,master_id);
          epoch_ += 1;
          if(epoch_ >= (MASTER_EPOCH + 25)) {
            /* slave exit slightly later than the master */
            this->exit_rpc_handler(0,0,NULL,NULL);
            /* shall not return back! */
          }
#endif
        } // end slave's case
      }   // end forever loop
    }

    void BenchListener::start_rpc_handler(int id,int cid,char *msg,void *arg) {

      clock_gettime(CLOCK_REALTIME,&start_t);
      fprintf(stdout,"[LISTENER] receive start RPC.\n");
      //sleep(1);
      try {
        for(auto it = workers_->begin();it != workers_->end();++it) {
          (*it)->routine_v1();
        }
      }
      catch(...) {
        assert(false);
      }
      inited_ = true;
    }

    void BenchListener::exit_rpc_handler(int id,int cid, char *msg, void *arg) {

      running = false;
      for(auto it = workers_->begin();it != workers_->end();++it) {
        (*it)->end_routine();
      }
      ending_record();
      ending();
    }

    void BenchListener::sigint_handler(int) {
      running = false;
      for(auto it = workers_->begin();it != workers_->end();++it) {
        (*it)->end_routine();
      }
    }

    void BenchListener::get_result_rpc_handler(int id, int cid,char *msg, void *arg) {
#if 1
      try {
        if(id != current_partition) {
          fprintf(stdout,"get results from %s , total %d\n",cm->network_[id].c_str(),n_returned_);
          reporter_->merge_data(msg);
          n_returned_ += 1;
        }
        if(n_returned_ == total_partition - 1) {
          epoch_ += 1;
          /* Calculate the first server's performance */
          char *buffer = new char[reporter_->data_len() + CACHE_LINE_SZ];
          reporter_->collect_data(buffer,start_t);
          reporter_->merge_data(buffer);
          free(buffer);
#ifdef LOG_RESULTS
          reporter_->report_data(epoch_,log_file);
#endif
          n_returned_ = 0;
        }
      } catch(...) {
        fprintf(stdout,"[Listener]: Report RPC error.\n");
        assert(false);
      }
#endif
    }

    void BenchListener::ending() {

      sleep(1);
      if(current_partition == 0) {
        uint64_t *test_ptr = (uint64_t *)rdma_buffer;
        fprintf(stdout,"sanity checks...%lu\n",*test_ptr);
      }
      fprintf(stdout,"Benchmark ends... \n");
      ending_record();
      reporter_->end();
      // delete cm;
      /* wait for ending message to pass */
      exit(0);
    }

    void BenchListener::ending_record() {

      //if(current_partition != 0) return;
      // some performance constants
      auto second_cycle = util::Breakdown_Timer::get_one_second_cycle();
      fprintf(stdout,"report ending\n");
      if(1) {
#if CS == 1
        int cid = nthreads;
#if SI_TX == 1
        cid += 1;
#endif
        fprintf(stdout,"use tcid %d, total workers %d\n",cid,workers_->size());
        BenchClient *c = (BenchClient *)((*workers_)[cid]);
        auto &timer = c->timer_;
#else
        auto &timer = (*workers_)[0]->latency_timer_;
#endif
        timer.calculate_detailed();
        auto m_l = timer.report_medium() / second_cycle * 1000;
        auto m_9 = timer.report_90() / second_cycle * 1000;
        auto m_99 = timer.report_99() / second_cycle * 1000;
        auto m_av = timer.report_avg() / second_cycle * 1000;
        fprintf(stdout,"Medium latency %3f ms, 90th latency %3f ms, 99th latency %3f ms, avg %3f ms\n",
                m_l,m_9,m_99,m_av);
#ifdef LOG_RESULTS
        log_file << m_l << " " << m_9<<" " << m_99 <<" "<<m_av<<std::endl;
        log_file.close();
#endif

      }


    }
  } // end namespace oltp
}; //end namespace nocc
