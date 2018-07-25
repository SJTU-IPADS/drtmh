#pragma once

#include "core/rdma_sched.h"
#include "rdmaio.h"

namespace nocc {
namespace rtx {

using namespace rdmaio;
using namespace oltp;

class RDMALogger : public Logger {
 public:
  RDMALogger(RdmaCtrl *cm, RDMA_sched* rdma_sched,int nid,int tid,uint64_t base_off,
             RRpc *rpc,int ack_rpc_id,
             int expected_store,char *local_p,int ms,int ts,int size,int entry_size = RTX_LOG_ENTRY_SIZE)
      :Logger(rpc,ack_rpc_id,base_off,expected_store,local_p,ms,ts,size,entry_size),
       node_id_(nid),worker_id_(tid),
       scheduler_(rdma_sched)
  {
    // init local QP vector
    for(uint i = 0;i < ms;++i) {
      auto qp = cm->get_rc_qp(tid,i,0);
      assert(qp != NULL);
      qp_vec_.push_back(qp);
    }
  }

  inline void log_remote(BatchOpCtrlBlock &clk, int cor_id) {

    int size = clk.batch_msg_size(); // log size
    assert(clk.mac_set_.size() > 0);
    // post the log to remote servers
    for(auto it = clk.mac_set_.begin();it != clk.mac_set_.end();++it) {
      int  mac_id = *it;
      auto qp = qp_vec_[mac_id];
      auto off = mem_.get_remote_log_offset(node_id_,worker_id_,mac_id,size);
      assert(off != 0);
      qp->rc_post_send(IBV_WR_RDMA_WRITE,clk.req_buf_,size,off,
                       IBV_SEND_SIGNALED | ((size < 64)?IBV_SEND_INLINE:1),cor_id);
      scheduler_->add_pending(cor_id,qp);
    }
    // Requires yield call after this!
  }

 private:
  std::vector<Qp *> qp_vec_;
  RDMA_sched *scheduler_;
  int node_id_;
  int worker_id_;
};

}; // namespace rtx
}; // namespace nocc
