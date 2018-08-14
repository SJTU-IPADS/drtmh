#pragma once

#include "core/rdma_sched.h"
#include "rdmaio.h"

namespace nocc {
namespace rtx {

using namespace rdmaio;
using namespace oltp;

class RDMALogger : public Logger {
 public:
  RDMALogger(RdmaCtrl *cm, RScheduler* rdma_sched,int nid,int tid,uint64_t base_off,
             RRpc *rpc,int ack_rpc_id,
             int expected_store,char *local_p,int ms,int ts,int size,int entry_size = RTX_LOG_ENTRY_SIZE)
      :Logger(rpc,ack_rpc_id,base_off,expected_store,local_p,ms,ts,size,entry_size),
       node_id_(nid),worker_id_(tid),
       scheduler_(rdma_sched)
  {
    // init local QP vector
    fill_qp_vec(cm,worker_id_);
  }

  inline void log_remote(BatchOpCtrlBlock &clk, int cor_id) {

    int size = clk.batch_msg_size(); // log size
    assert(clk.mac_set_.size() > 0);
    // post the log to remote servers
    for(auto it = clk.mac_set_.begin();it != clk.mac_set_.end();++it) {
      int  mac_id = *it;
      //auto qp = qp_vec_[mac_id];
      auto qp = get_qp(mac_id);
      auto off = mem_.get_remote_log_offset(node_id_,worker_id_,mac_id,size);
      assert(off != 0);
      scheduler_->post_send(qp,cor_id,
                            IBV_WR_RDMA_WRITE,clk.req_buf_,size,off,
                            IBV_SEND_SIGNALED | ((size < 64)?IBV_SEND_INLINE:1));
    }
    // requires yield call after this!
  }

 private:
  RScheduler *scheduler_;
  int node_id_;
  int worker_id_;

#include "qp_selection_helper.h"
};

}; // namespace rtx
}; // namespace nocc
