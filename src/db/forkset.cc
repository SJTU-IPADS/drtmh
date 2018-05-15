#include "all.h"
#include "forkset.h"

#include "./framework/req_buf_allocator.h"

namespace nocc {

  using namespace oltp;  // the framework namespace
  namespace oltp {
    extern __thread oltp::RPCMemAllocator * msg_buf_alloctors;
  }

  namespace db {

    ForkSet::ForkSet (RRpc *rpc,int cid) : rpc_(rpc), cor_id_(cid) {
      reply_buf_  = (char *)malloc(4096);
      server_num_ = 0;
    }

    ForkSet::~ForkSet() {
      clear();
    }

    void ForkSet::clear() {
      free(reply_buf_);
      msg_buf_alloctors[cor_id_].rollback_buf();
    }

    void ForkSet::reset() {
      msg_buf_end_   = msg_buf_start_;
    }

    void  ForkSet::do_fork() {
      assert(server_num_ == 0);
    }
  
    char *ForkSet::do_fork(int sizeof_header) {
      msg_buf_start_ = msg_buf_alloctors[cor_id_].get_req_buf();
      msg_buf_end_   = msg_buf_start_ + sizeof_header;
      return msg_buf_start_;
    }

    void ForkSet::add(int pid) {
      if(server_set_.find(pid) == server_set_.end()) {
        server_set_.insert(pid);
        server_lists_[server_num_++] = pid;
      }
    }
  
    char *ForkSet::add(int pid,int sizeof_payload) {
      char *ret = msg_buf_end_;
      msg_buf_end_ += sizeof_payload;
      add(pid);
      return ret;
    }

    int ForkSet::fork(int id,int type) {
      assert(cor_id_ != 0);
      //return rpc_handler_->send_reqs(id,msg_buf_end_ - msg_buf_start_, reply_buf_,
      //                             server_lists_,server_num_,this->cor_id_,type);

      rpc_->prepare_multi_req(reply_buf_,server_num_,cor_id_);
      rpc_->broadcast_to(msg_buf_start_,id,
                         msg_buf_end_ - msg_buf_start_,
                         cor_id_,type,
                         server_lists_,server_num_);
      return server_num_;
    }

    int ForkSet::fork(int id,char *msg,int size,int type) {
      msg_buf_start_ = msg_buf_alloctors[cor_id_].get_req_buf();
      memcpy(msg_buf_start_,msg,size);
      rpc_->prepare_multi_req(reply_buf_,server_num_,cor_id_);
      rpc_->broadcast_to(msg_buf_start_,id,
                         size,
                         cor_id_,type,
                         server_lists_,server_num_);
      return server_num_;
    }
  };

};
