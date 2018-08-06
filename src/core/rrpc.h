#ifndef NOCC_DB_RRPC_H
#define NOCC_DB_RRPC_H

#include "common.h"

#include "all.h"
#include "framework/config.h" // to be fixed later!
#include "msg_handler.h"

#include "ralloc.h" // RDMA malloc

#include "logging.h"

#define MAX_RPC_SUPPORT 16

#include <functional>

//
namespace nocc {

namespace oltp {

class RScheduler;
class RRpc {

  // The RPC callback takes 4 parameters:
  // calling server's id, calling coroutine id, a pointer to the msg, and an extra(typically not used argument)
  typedef std::function<void(int,int,char *,void *)> rpc_func_t;
 public:
  // Three type of message exchanged by the RRpc.c
  enum TYPE {
    // Yield RPC allocates some context for executing such RPC callback.
    // It's a little slower than one-shot RPC, but supports more functionality.
    REQ = 0,  // a one-shot RPC request
    Y_REQ,    // a yield RPC request
    REPLY     // the reply of a particular request
  };

  // the RPC header used in rrpc
  struct rrpc_header {
    struct rrpc_meta {
      uint32_t type : 2;
      uint32_t rpc_id :  5;
      uint32_t payload : 18;
      uint32_t cid     : 7;
    } meta;
#ifdef RPC_CHECKSUM
    uint64_t  counter;
    uint64_t  checksum;
#endif
  }  __attribute__ ((aligned (sizeof(uint64_t))));

  RRpc(int tid, int cs, int req_buf_num = MAX_INFLIGHT_REQS, int reply_buf_num = MAX_INFLIGHT_REPLY);

  inline bool has_pending_reqs(int cid) {
    return pending_reqs(cid) != 0;
  }

  inline int pending_reqs(int cid) {
    return reply_counts_[cid];
  }

  void set_msg_handler(rdmaio::MsgHandler *msg) {
    msg_handler_ = msg;
    msg_padding_ = msg->msg_padding();
  }

  void register_callback(rpc_func_t callback,int id,bool overwrite = false) {
    // register an RPC to a specificed ID
    assert(id >= 0 && id < MAX_RPC_SUPPORT); // check id
    if(!overwrite)
      assert(!register_[id]);
    else {
      // warning !!
    }
    callbacks_[id] = callback;
    register_[id]  = true;
  }

  // The hook used to receive message.
  // It parse in-coming network messages (from msg_handler_),
  // and calls specific app callback, if necessary.
  bool  poll_comp_callback(char *msg,int nid,int tid);

  // Meta-data reserved for each message. It includes a header, and some implementation specific padding
  inline int rpc_padding() const { return msg_padding_ + sizeof(rrpc_header);}

  // Get a buffer for sending RPC request. The buffer will not be allocate to other
  inline char *get_static_buf(int size) {
    char *res =  ((char *)Rmalloc(rpc_padding() + size)) + rpc_padding();
    assert(res != NULL);
    return res;
  }

  inline void  free_static_buf(char *ptr) { Rfree(ptr - rpc_padding());}

  // Get a reply buffer for the RPC handler
  inline char *get_reply_buf() {
    auto res = reply_buf_pool_[(reply_buf_slot_++) % reply_buf_pool_.size()];
    return res + rpc_padding();
  }

  // Get a buffer for sending RPC request.
  // This buffer can be re-used.
  inline char *get_fly_buf(int cid) {
    char *res = req_buf_pool_[cid][(req_buf_slots_[cid]++) % req_buf_pool_[cid].size()];
    return res + rpc_padding();
  }

  inline void prepare_header(char *msg, int rpc_id,int size,int cid,int type) {
    volatile rrpc_header *header = (rrpc_header *) (msg - sizeof(rrpc_header));
    header->meta.type = type;
    header->meta.payload = size;
    header->meta.cid = cid;
    header->meta.rpc_id  = rpc_id;
  }

  inline void prepare_multi_req(char *reply_buf,int num_of_replies,int cid) {
    assert(num_of_replies > 0);
    reply_bufs_[cid] = reply_buf;  // the buffer to hold responses
    reply_counts_[cid] += num_of_replies; // the number of replies to receive
  }

  inline void append_req(char *msg,
                         int rpc_id,int size,int cid,int type,
                         int server_id) {
    return append_req(msg,rpc_id,size,cid,type,server_id,worker_id_);
  }

  inline void append_req(char *msg,
                         int rpc_id,int size,int cid,int type,
                         int server_id,int server_tid) {
    prepare_header(msg,rpc_id,size,cid,type);
    msg_handler_->send_to(server_id,server_tid,
                          (char *)(msg - rpc_padding()),size + sizeof(rrpc_header));
  }

  inline void append_pending_req(char *msg,
                                 int rpc_id,int size,int cid,int type,
                                 int server_id) {
    return append_pending_req(msg,rpc_id,size,cid,type,server_id,worker_id_);
  }

  inline void append_pending_req(char *msg,
                                 int rpc_id,int size,int cid,int type,
                                 int server_id,int server_tid) {
    prepare_header(msg,rpc_id,size,cid,type);
    msg_handler_->post_pending(server_id,server_tid,
                               (char *)(msg - rpc_padding()),size + sizeof(rrpc_header));
  }

  inline void flush_pending() { msg_handler_->flush_pending(); }

  inline void broadcast_to(char *msg,
                           int rpc_id, int size,int cid,int type,
                           int *server_lists, int num) {
    prepare_header(msg,rpc_id,size,cid,type);
    msg_handler_->broadcast_to(server_lists,num,
                               (char *)(msg - rpc_padding()),size + sizeof(rrpc_header));
  }

  inline void broadcast_to(char *msg,int rpc_id,int size,int cid,int type,const std::set<int> &server_set) {
    prepare_header(msg,rpc_id,size,cid,type);
    msg_handler_->broadcast_to(server_set,(char *)(msg - rpc_padding()),size + sizeof(rrpc_header));
  }

  inline void send_reply(char *msg,int size,int server_id,int cid) {
    ASSERT(size + rpc_padding() < 1024) << " send size " << size;
    return send_reply(msg,size,server_id,worker_id_,cid);
  }

  inline void send_reply(char *msg,int size,int server_id,int server_tid,int cid) {
    return append_pending_req(msg,0,size,cid,REPLY,server_id,server_tid);
  }

  // uses a specific handler to send the reply
  inline void send_reply(char *msg,int size,int server_id,int server_tid,int cid,
                         rdmaio::MsgHandler *handler) {
    prepare_header(msg,0,size,cid,REPLY);
    handler->post_pending(server_id,server_tid,
                          (char *)(msg - rpc_padding()),size + sizeof(rrpc_header));
  }

  // thread id of the RPC handler
  const int worker_id_;

 private:
  rpc_func_t callbacks_[MAX_RPC_SUPPORT];
  bool       register_[MAX_RPC_SUPPORT];

  // msg handler related structures
  int8_t msg_padding_;
  rdmaio::MsgHandler * msg_handler_;

  // buffer pools
  std::vector<char *> reply_buf_pool_;
  std::vector<std::vector<char *> >req_buf_pool_;
  uint16_t reply_buf_slot_ = 0;
  std::vector<uint8_t> req_buf_slots_;

  // reply data structures, used for replying message
  char        **reply_bufs_ ;
  static __thread int *reply_counts_;

  // some statics count
  uint64_t processed_rpc_ = 0;

  friend class RScheduler;
  DISABLE_COPY_AND_ASSIGN(RRpc);
}; // class rrpc

// bind body->function to rpc[id]
#define ROCC_BIND_STUB(rpc,function,body,id)                            \
  (rpc)->register_callback(std::bind(function,(body),                   \
                                     std::placeholders::_1,             \
                                     std::placeholders::_2,             \
                                     std::placeholders::_3,             \
                                     std::placeholders::_4),(id),true);

}   // namespace oltp
}   // namespace nocc

#endif
