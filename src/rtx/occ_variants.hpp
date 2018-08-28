// This file implements various of OCC protocol on top two-sided~(messaging) primitives

#include "occ.h"

namespace nocc {

namespace rtx {

// Add FaSST's optimizations, such as logging, merging execution
class OCCFast : public OCC {
 public:
  OCCFast(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int cid,int response_node)
      :OCC(worker,db,rpc_handler,nid,cid,response_node) {

  }

  bool parse_batch_result(int num) {

    char *ptr  = reply_buf_;
    for(uint i = 0;i < num;++i) {
      // parse a reply header
      ReplyHeader *header = (ReplyHeader *)(ptr);
      ptr += sizeof(ReplyHeader);
      for(uint j = 0;j < header->num;++j) {
        OCCResponse *item = (OCCResponse *)ptr;
        if(unlikely(item->seq == 0)) {
          // abort case
          abort_ = true;
        }
        //read_set_[item->idx].data_ptr = ptr + sizeof(OCCResponse);

        read_set_[item->idx].data_ptr = (char *)malloc(read_set_[item->idx].len);
        memcpy(read_set_[item->idx].data_ptr, ptr + sizeof(OCCResponse),read_set_[item->idx].len);

        read_set_[item->idx].seq      = item->seq;
        write_batch_helper_.add_mac(read_set_[item->idx].pid);
        ptr += (sizeof(OCCResponse) + item->payload);
      }
    }
    return true;
  }

  void prepare_write_contents_f() {

    write_batch_helper_.clear();

    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      add_batch_entry<RtxWriteItem>(write_batch_helper_,
                                    (*it).pid,
                                    /* init write item */ (*it).pid,(*it).tableid,(*it).key,(*it).len);
      memcpy(write_batch_helper_.req_buf_end_,(*it).data_ptr,(*it).len);
      write_batch_helper_.req_buf_end_ += (*it).len;
    }
  }

  bool commit(yield_func_t &yield) {

    bool ret = true;
    if(abort_) {
      goto ABORT;
    }

    prepare_write_contents_f();

    log_remote(yield); // log remote using *logger_*
    write_back_oneshot(yield);
    gc_readset(); gc_writeset();
    return true;
 ABORT:
    fast_release_writes(yield);
    gc_readset(); gc_writeset();
    return false;
  }

  void fast_release_writes(yield_func_t &yield) {

    start_batch_rpc_op(write_batch_helper_);
    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if((*it).pid != node_id_) { // remote case
        add_batch_entry<RtxLockItem>(write_batch_helper_, (*it).pid,
                                     /*init RTXLockItem */ (*it).pid,(*it).tableid,(*it).key,(*it).seq);
      }
      else {
        assert(false); // not implemented
      }
    }
    send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);

    worker_->indirect_yield(yield);
  }
};

} // namespace rtx

} // namespace nocc
