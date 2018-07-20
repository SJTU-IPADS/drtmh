// This file implements various of OCC protocol on top two-sided~(messaging) primitives

#include "rtx_occ.h"


namespace nocc {

namespace rtx {

// Add FaSST's optimizations, such as logging, merging execution
class RtxOCCFast : public RtxOCC {
 public:
  RtxOCCFast(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int cid,int response_node)
      :RtxOCC(worker,db,rpc_handler,nid,cid,response_node) {

  }

  bool parse_batch_result(int num) {

    char *ptr  = reply_buf_;
    for(uint i = 0;i < num;++i) {
      // parse a reply header
      RtxReplyHeader *header = (RtxReplyHeader *)(ptr);
      ptr += sizeof(RtxReplyHeader);
      for(uint j = 0;j < header->num;++j) {
        RtxOCCResponse *item = (RtxOCCResponse *)ptr;
        if(unlikely(item->seq == 0)) {
          // abort case
          abort_ = true;
        }
        read_set_[item->idx].data_ptr = ptr + sizeof(RtxOCCResponse);
        read_set_[item->idx].seq      = item->seq;
        ptr += (sizeof(RtxOCCResponse) + item->payload);
      }
    }
    return true;
  }

  bool commit(yield_func_t &yield) {
    bool ret = true;
    if(abort_) {
      gc_writeset();
      gc_readset();
      goto ABORT;
    }
    if(unlikely(!validate_reads(yield))){
      gc_writeset();
      goto ABORT;
    }
    write_back(yield);
    return true;
 ABORT:
    fast_release_writes(yield);
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
