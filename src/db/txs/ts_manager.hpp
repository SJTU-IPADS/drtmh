#ifndef NOCC_DB_TS_MANAGER
#define NOCC_DB_TS_MANAGER

#include <functional>
#include <vector>
#include <stdint.h>
#include <queue>   // priority queue for sorting TS updates
#include <zmq.hpp>

#include "rocc_config.h"
#include "rdmaio.h"
#include "ralloc.h"
#include "all.h"

#include "core/rworker.h"
#include "framework/req_buf_allocator.h"

#include "framework/config.h"

extern size_t nthreads;
extern int tcp_port;

namespace nocc {

namespace oltp {
extern __thread RPCMemAllocator *msg_buf_alloctors;
}
using namespace oltp;

extern zmq::context_t send_context;
extern std::vector<SingleQueue *>   local_comm_queues;

namespace db {

#define SI_TS_MASK  (0xffffffffffff)
#define SI_SERVER_MASK (0xffff)

#define SI_GET_SERVER(x)   (((x) >> 48) & SI_SERVER_MASK)
#define SI_GET_COUNTER(x)  ((x) & SI_TS_MASK)
#define SI_ENCODE_TS(s,t)  (((s) << 48) | (t))

#define LARGE_VEC 0
#define ONE_CLOCK 1
//#define EM_OCC // OCC + TS

#define TS_USE_MSG 1
#define RPC_TS_GET 7
#define RPC_TS_UPDATE 8
#define RPC_TS_ACQUIRE 10

#if ONE_CLOCK
#undef LARGE_VEC
#define LARGE_VEC 0
#define EM_OCC
#endif

using namespace rdmaio;

class TSManager : public RWorker {
  typedef std::priority_queue<int, std::vector<uint64_t> > ts_queue_t_;
 public:
  struct UpdateArg {
    uint16_t thread_id;
    uint64_t counter;
  };

#if LARGE_VEC == 1
  static __thread uint64_t local_ts;
#else
  static uint64_t local_ts;
#endif
  static uint64_t last_ts;

 public:
  TSManager(int worker_id,RdmaCtrl *cm,int master_id,uint64_t ts_addr)
      :RWorker(worker_id,cm),
#if LARGE_VEC == 1
       ts_size_(sizeof(uint64_t) * cm->get_num_nodes() * nthreads),
#elif ONE_CLOCK == 1
       ts_size_(sizeof(uint64_t)),
#else
       ts_size_(sizeof(uint64_t) * cm->get_num_nodes()),
#endif
       master_id_(master_id), cm_(cm),ts_addr_(ts_addr)
  {
#if LARGE_VEC == 0
    local_ts = 3;
    last_ts  = local_ts - 1;
#endif
    // allocte TS for TS fetch
    RThreadLocalInit();
    fetched_ts_buffer_ = (char *)Rmalloc(ts_size_);
    memset(fetched_ts_buffer_,0,ts_size_);

    uint64_t *set_ptr = (uint64_t *)fetched_ts_buffer_;
    uint64_t *ts_buffer = (uint64_t *)((char *)(cm_->conn_buf_) + ts_addr_);

    assert(set_ptr != NULL && ts_buffer != NULL);
    init_ts_meta(ts_buffer); // global copy of the TS
    init_ts_meta(set_ptr);   // local copy of the TS

    running = true;
#if ONE_CLOCK == 0 && LARGE_VEC == 0 && TS_USE_MSG == 1
    for(uint i = 0;i < cm->get_num_nodes();++i) {
      update_locks_.push_back(std::mutex());
      pending_tss_.push_back(ts_queue_t_());
    }
#endif
  }

  virtual void run() {

    fprintf(stdout,"[Global sequence running] !\n");
    init_routines(1); // only 1 routine is enough

#if USE_RDMA
    init_rdma();
    create_qps();
#endif

    MSGER_TYPE type = UD_MSG;
#if USE_TCP_MSG == 1
    create_tcp_connections(local_comm_queues[worker_id_],tcp_port,send_context);
#else
    create_rdma_ud_connections(1);
#endif

    rpc_->register_callback(std::bind(&TSManager::ts_get_handler,this,
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      std::placeholders::_3,
                                      std::placeholders::_4),RPC_TS_GET);
    rpc_->register_callback(std::bind(&TSManager::ts_update_handler,this,
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      std::placeholders::_3,
                                      std::placeholders::_4),RPC_TS_UPDATE);

    start_routine();
  }

  virtual void worker_routine(yield_func_t &yield) {

    char *temp = (char *)Rmalloc(ts_size_);
    char *local_buffer = (char *)Rmalloc(ts_size_);
    memset(temp,0,ts_size_);
    memset(local_buffer,0,ts_size_);

#if LARGE_VEC == 1
    local_ts = 3; // This is a thread local init
#endif

#if USE_RDMA
    Qp *qp = cm_->get_rc_qp(worker_id_,master_id_);
#else
    Qp *qp  = NULL;
#endif
    // prepare the buffer for sending RPC
    char *req_buf = rpc_->get_static_buf(64);
    init_ts_meta((uint64_t *)fetched_ts_buffer_);

    while(running) {
#if TS_USE_MSG
      // use message to fetch TS
      rpc_->prepare_multi_req(local_buffer,1,cor_id_);
      rpc_->append_req(req_buf,RPC_TS_GET,sizeof(uint64_t),cor_id_,RRpc::REQ,master_id_);
#else
      // use one-sided READ for records
      qp->rc_post_send(IBV_WR_RDMA_READ,(char *)local_buffer,ts_size_,ts_addr_,IBV_SEND_SIGNALED,cor_id_);
      rdma_sched_->add_pending(cor_id_,qp);
#endif
      indirect_yield(yield);
      // got results back
      memcpy(temp,local_buffer,ts_size_);
      // swap the ptr
      char *swap = temp;
      temp = fetched_ts_buffer_;
      fetched_ts_buffer_ = swap;
    }
    Rfree(local_buffer);
  } // a null worker routine,since no need for yield

  uint64_t get_commit_ts() {
    return __sync_fetch_and_add(&local_ts,1);
  }

  uint64_t get_start_ts(char *buffer) {
    memcpy(buffer,fetched_ts_buffer_,ts_size_);
  }

  void  init_ts_meta(uint64_t *ts_buffer) {
    for(uint i(0),j(0);j < ts_size_;j += sizeof(uint64_t),i++) ts_buffer[i] = 2;
  }

  void print_ts(uint64_t *ts_buffer) {
    char *ts_ptr = (char *)ts_buffer;
    for(uint printed = 0; printed < ts_size_; printed += sizeof(uint64_t)) {
      fprintf(stdout,"%lu\t",*((uint64_t *)(ts_ptr + printed)));
    }
    fprintf(stdout,"\n");
  }

  void ts_get_handler(int id,int cid,char *msg,void *temp) {
    uint64_t *ts_buffer = (uint64_t *)((char *)(cm_->conn_buf_) + ts_addr_);
    char *reply = rpc_->get_reply_buf();
    memcpy(reply,ts_buffer,ts_size_);
    rpc_->send_reply(reply,ts_size_,id,worker_id_,cid);
  }

  void ts_update_handler(int id,int cid,char *msg,void *temp) {

    UpdateArg *arg = (UpdateArg *)msg;
    uint64_t *ts_buffer = (uint64_t *)((char *)(cm_->conn_buf_) + ts_addr_);

    assert(arg->thread_id >= 0 && arg->thread_id < nthreads);
    assert(id >= 0 && id < 64);
    //fprintf(stdout,"%d, update %lu\n",id,arg->counter);
#if ONE_CLOCK == 1
    id = 0; // only update the first entry
#endif

#if LARGE_VEC == 1
    ts_buffer[arg->thread_id + id * nthreads] = arg->counter;
#elif ONE_CLOCK
    update_lock_.lock();
    pending_ts_.push(arg->counter);
    while(pending_ts_.top() == ts_buffer[0] - 1) {
      ts_buffer[0] += 1;
      pending_ts_.pop();
    }
    update_lock_.unlock();
#else
    // per mac ts entry
    assert(false);
#endif
  }

  // members
 public:
  const int ts_size_;
  const int master_id_;
  const uint64_t ts_addr_;
 private:
  RdmaCtrl *cm_;
  char *fetched_ts_buffer_;
#if ONE_CLOCK
  std::mutex update_lock_;
  ts_queue_t_ pending_ts_;
#endif
#if ONE_CLOCK == 0 && LARGE_VEC == 0
  std::vector<std::mutex>  update_locks_;
  std::vector<ts_queue_t_> pending_tss_;
#endif
}; // end class

};
};

#endif
