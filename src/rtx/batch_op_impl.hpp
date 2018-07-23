#include "msg_format.hpp"

namespace nocc {

using namespace nocc::util;

namespace rtx {

struct BatchOpCtrlBlock {

  char *req_buf_;
  char *req_buf_end_;
  char *reply_buf_;
  std::set<int> mac_set_;
  int batch_size_;

  inline BatchOpCtrlBlock(char *req_buf,char *res_buf) :
      batch_size_(0),
      req_buf_(req_buf),
      reply_buf_(res_buf)
  {
    clear();
  }

  inline void add_mac(int pid) {
    mac_set_.insert(pid);
  }

  inline void clear() {
    mac_set_.clear();
    req_buf_end_ = req_buf_ + sizeof(RTXRequestHeader);
    batch_size_ = 0;
  }

  inline void clear_buf() {
    req_buf_end_ = req_buf_ + sizeof(RTXRequestHeader);
    batch_size_ = 0;
  }

  inline int batch_msg_size() {
    return req_buf_end_ - req_buf_;
  }

  inline int send_batch_op(RRpc *rpc,int cid,int rpc_id,bool pa = false) {
    if(batch_size_ > 0) {
      ((RTXRequestHeader *)req_buf_)->num = batch_size_;
      if(!pa) {
        rpc->prepare_multi_req(reply_buf_,mac_set_.size(),cid);
      }
      rpc->broadcast_to(req_buf_,rpc_id,
                        batch_msg_size(),
                        cid,RRpc::REQ,mac_set_);
    }
    return mac_set_.size();
  }
};

inline __attribute__((always_inline))
void TXOpBase::start_batch_rpc_op(BatchOpCtrlBlock &ctrl) {
  // no pending batch requests
  ctrl.clear();
}

template <typename REQ,typename... _Args> // batch req
inline __attribute__((always_inline))
void TXOpBase::add_batch_entry(BatchOpCtrlBlock &ctrl,int pid, _Args&& ... args) {

  ctrl.batch_size_ += 1;
  // copy the entries
  *((REQ *)ctrl.req_buf_end_) = REQ(std::forward<_Args>(args)...);
  ctrl.req_buf_end_ += sizeof(REQ);

  ctrl.mac_set_.insert(pid);
}

template <typename REQ,typename... _Args> // batch req
inline __attribute__((always_inline))
void TXOpBase::add_batch_entry_wo_mac(BatchOpCtrlBlock &ctrl,int pid, _Args&& ... args) {
  ctrl.batch_size_ += 1;

  // copy the entries
  *((REQ *)ctrl.req_buf_end_) = REQ(std::forward<_Args>(args)...);
  ctrl.req_buf_end_ += sizeof(REQ);
}

inline  __attribute__((always_inline))
int TXOpBase::send_batch_rpc_op(BatchOpCtrlBlock &ctrl,int cid,int rpc_id,bool pa) {
  return ctrl.send_batch_op(rpc_,cid,rpc_id,pa);
}

template <typename REPLY>
inline  __attribute__((always_inline))
REPLY *TXOpBase::get_batch_res(BatchOpCtrlBlock &ctrl,int idx) {
  return ((REPLY *)ctrl.reply_buf_ + idx);
}

}; // namespace rtx

}; // namespace nocc
