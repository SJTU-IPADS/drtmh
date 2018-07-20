#pragma once

#include "core/rdma_sched.h"
#include "rdmaio.h"

namespace nocc {

namespace rtx {

using namespace rdmaio;
using namespace oltp;

class RpcLogger : public Logger {
 public:
  RpcLogger(RRpc *rpc,int log_rpc_id,int ack_rpc_id,
            int expected_store,char *local_p,int ms,int ts,int size,int entry_size = RTX_LOG_ENTRY_SIZE)
      :Logger(rpc,ack_rpc_id,expected_store,local_p,ms,ts,size,entry_size),
       log_rpc_id_(log_rpc_id)
  {
    // register RPC if necessary
    rpc->register_callback(std::bind(&RpcLogger::log_remote_handler,this,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3,
                                     std::placeholders::_4),
                           log_rpc_id_,true);
  }

  inline void log_remote(BatchOpCtrlBlock &clk,int cor_id) {
    clk.send_batch_op(rpc_handler_,cor_id,log_rpc_id_,false);
  }

  void log_remote_handler(int id,int cid,char *msg,void *arg) {

    int size = (uint64_t)arg;
    char *local_ptr = mem_.get_next_log(id,rpc_handler_->worker_id_,size);

    assert(size < RTX_LOG_ENTRY_SIZE);
    memcpy(local_ptr,msg,size);

    char* reply_msg = rpc_handler_->get_reply_buf();
    rpc_handler_->send_reply(reply_msg,0,id,cid); // a dummy reply
  }

 private:
  const int log_rpc_id_;
};


}  // namespace rtx
}  // namespace nocc
