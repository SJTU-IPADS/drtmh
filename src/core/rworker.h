#ifndef NOCC_OLTP_BENCH_WORKER_H
#define NOCC_OLTP_BENCH_WORKER_H

#include "all.h"
#include "./utils/thread.h"
#include "./utils/util.h"

#include "rrpc.h"
#include "commun_queue.hpp"
#include "ud_msg.h"
#include "rdma_sched.h"
#include "routine.h"

#include <vector>
#include <string>
#include <stdint.h>

#include <zmq.hpp>

using namespace rdmaio;
using namespace rdmaio::udmsg;

namespace nocc {

namespace oltp {

#define INDIRECT_YIELD(yield) RWorker::thread_worker->indirect_yield(yield);
#define DIRECT_YIELD(yield)   RWorker::thread_worker->yield_next(yield);

// abstract worker
class RWorker : public ndb_thread {
 public:

  // communication type supported by the Worker
  enum MSGER_TYPE {
    UD_MSG, RC_MSG, TCP_MSG
  };

  RWorker(int worker_id,RdmaCtrl *cm,uint64_t seed = 0)
      :cm_(cm),
       worker_id_(worker_id),
       rand_generator_(seed) // the random generator used at this thread
  {
  }

  /**
   * A void run() should be overloaded, as the main thread body.
   * This is defined in ndb_thread.
   */

  // main function for each routine, shall be overwritten
  virtual void worker_routine(yield_func_t &yield) = 0;

  // a handler be called after exit
  virtual void exit_handler() {

  }

  // called after change context to cor_id
  virtual void change_ctx(int cor_id) {

  }

  // choose a NIC port to use, update use_port_
  virtual int  choose_rnic_port();

  // init functions provided
  // the init shall be called sequentially
  void init_routines(int coroutines);

  void init_rdma();

  void create_qps(int num  = 0); // depends init_rdma

  void create_rdma_ud_connections(int total_connections = 1);

  void create_rdma_rc_connections(char *start_buffer,uint64_t total_ring_sz,uint64_t total_ring_padding);

  void create_tcp_connections(util::SingleQueue *queue, int tcp_port, zmq::context_t &context);

  void create_client_connections(int total_connections = 1); // depends init_rdma

  // create a mapped log for logging to file (for debug)
  void create_logger();

  // Init Worker to a ready start status
  // This is used, such as in a global barrier.
  void routine_v1() {
    running = true;
  }

  // Really start the routine
  void start_routine() {
    assert(inited == true);

    // pre-checks
    for(uint it = 0;it < total_worker_coroutine;++it) {
      assert(rpc_->has_pending_reqs(it) == false);
      assert(rdma_sched_->pending_counts_[it] == 0);
    }
    routines_[0]();
  }

  // Set routine in a ending status.
  // The implementation asynchronously check the ending status.
  void end_routine() {
    running = false;
  }

  // handlers communication events,
  // such as: in-comming RPC request/response; RDMA operation completions
  inline ALWAYS_INLINE
  void events_handler() const {
    if(client_handler_ != NULL)
      client_handler_->poll_comps(); // poll reqs from clients
    if(msg_handler_ != NULL)
      msg_handler_->poll_comps(); // poll rpcs requests/replies
    rdma_sched_->poll_comps();  // poll RDMA completions
  }

  void indirect_yield(yield_func_t &yield);
  void indirect_must_yield(yield_func_t &yield);
  void yield_next(yield_func_t &yield);

  // whether worker is running
  inline ALWAYS_INLINE
  bool running_status() const { return running; }

  // whether worker has finished initilization
  inline ALWAYS_INLINE
  bool init_status() const { return inited; }

  inline ALWAYS_INLINE
  int cor_id() const { return cor_id_; }

 public:
  const unsigned int worker_id_;  // thread id of the running routine
  RoutineMeta *routine_meta_ = NULL;

  util::fast_random rand_generator_;

 protected:
  unsigned int cor_id_ = 0;
  RdmaCtrl *cm_ = NULL;
  RRpc *rpc_    = NULL;
  RScheduler *rdma_sched_ = NULL;
  int       use_port_ = -1;  // which RNIC's device to use

  // running status
  bool   running = false;
  bool   inited  = false;


 private:
  MsgHandler *msg_handler_ = NULL;  // communication between servers
  UDMsg *client_handler_   = NULL;  // communication with clients
  MSGER_TYPE  server_type_ = UD_MSG;
  coroutine_func_t *routines_ = NULL;

  // coroutine related stuffs
  int    total_worker_coroutine = 0;

  void new_master_routine(yield_func_t &yield,int cor_id);

  void set_local_worker() {
    RWorker::thread_worker = this;
  }

 public:
  static __thread RWorker *thread_worker;

  DISABLE_COPY_AND_ASSIGN(RWorker);
};


} // end namespace oltp
} // end namespace nocc

#include "rworker_yield_impl.hpp"

#endif
