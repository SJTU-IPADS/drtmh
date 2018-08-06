#include "rrpc.h"

#include "routine.h"
#include "logging.h"

#include "rdma_sched.h"

namespace nocc {

namespace oltp {

RRpc::RRpc(int tid,int coroutines,int req_buf_num,int reply_buf_num)
    : worker_id_(tid),
      reply_buf_slot_(0)
{
  for(uint i = 0;i < MAX_RPC_SUPPORT;++i) register_[i] = false;

  // init buf
  assert(req_buf_num > 0 && reply_buf_num > 0);
  RThreadLocalInit();
  int req_msg_size = MAX_MSG_SIZE + rpc_padding();
  int reply_msg_size = 1024; assert(reply_msg_size > rpc_padding()); // we uses a fixed reply msg size

  for(uint i = 0;i < coroutines + 1;++i) {
    req_buf_slots_.push_back(0);
    req_buf_pool_.push_back(std::vector<char *> ());

    for(uint j = 0;j < req_buf_num;++j) {
      char *buf = (char *)Rmalloc(req_msg_size); assert(buf != NULL);
      req_buf_pool_[i].push_back(buf);
    }
  }

  for(uint i = 0;i < reply_buf_num;++i) {
    char *buf = (char *)Rmalloc(reply_msg_size); assert(buf != NULL);
    reply_buf_pool_.push_back(buf);
  }

  reply_bufs_       = new char*[1 + coroutines];
  reply_counts_     = new int[1 +   coroutines];

  std::fill_n(reply_counts_,1 + coroutines,0);
  std::fill_n(reply_bufs_,1 + coroutines,static_cast<char *>(NULL));
}

bool RRpc::poll_comp_callback(char *msg,int from,int from_t) {

  rrpc_header *header = (rrpc_header *) msg;

  if(header->meta.type == REQ) {
    // normal rpcs
    try {
      callbacks_[header->meta.rpc_id](from,header->meta.cid,msg + sizeof(rrpc_header),
                                      (void *)(header->meta.payload));
      processed_rpc_ += 1;
    } catch (...) {
      LOG(7) << "rpc called failed at " << worker_id_ << ";With rpc id "
             << header->meta.rpc_id;
    }
  } else if (header->meta.type == Y_REQ) {
    // copy the msg
    char *temp = (char *)malloc(header->meta.payload);
    memcpy(temp,msg + sizeof(rrpc_header),header->meta.payload);
    add_one_shot_routine(from,header->meta.cid,header->meta.rpc_id,temp);

  } else if (header->meta.type == REPLY) {
    // This is a reply
    ASSERT(header->meta.cid != 0);
    if(unlikely(reply_counts_[header->meta.cid] <= 0)) {
      LOG(7) << "receive a reply from cid" << header->meta.cid << " at worker " << worker_id_ << " @mac " << from
             << " which is not required." << "Total " << processed_rpc_ << " processed.";
    }

    char *buf = reply_bufs_[header->meta.cid];
    assert(header->meta.payload < 1024 - rpc_padding());
    memcpy(buf,msg + sizeof(rrpc_header),header->meta.payload);

    reply_bufs_[header->meta.cid] += header->meta.payload;

    reply_counts_[header->meta.cid] -= 1;
    if(reply_counts_[header->meta.cid] == 0
       && RScheduler::pending_counts_[header->meta.cid] == 0) { // avoid the chain from being added twice

      reply_bufs_[header->meta.cid] = NULL;
      add_to_routine_list(header->meta.cid);
    }
  } else {
    assert(false);
  }
  return true;
}

__thread int *RRpc::reply_counts_ = NULL;

} // namespace oltp

} // namespace nocc
