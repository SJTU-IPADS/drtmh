// This file implements various of OCC protocol on top two-sided~(messaging) primitives

#include "occ.h"
#include "occ_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {

// FaSST's protocol, with two-sided's implementation
class OCCFast : public OCC {
 public:
  OCCFast(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int cid,int response_node)
      :OCC(worker,db,rpc_handler,nid,cid,response_node) {
    if(rpc_handler->worker_id_ == 0)
      LOG(4) << "using FaSST variants";
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


//Using FaSST's protocol, but a hybrid implementation
class OCCFastR : public OCCR {
 public:
  OCCFastR(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
           RdmaCtrl *cm,RScheduler* rdma_sched,int ms)
      :OCCR(worker,db,rpc_handler,nid,tid,cid,response_node,
            cm,rdma_sched,ms) {
    ASSERT(RDMA_CACHE);
  }

  bool parse_batch_result(int num) { }

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
#if ONE_SIDED_READ
  int add_batch_write(int tableid,uint64_t key,int pid,int len,yield_func_t &yield) {
    char *data_ptr = (char *)Rmalloc(sizeof(MemNode) + len);
    ASSERT(data_ptr != NULL);

    auto off = rdma_lookup_op(pid,tableid,key,data_ptr,yield); // the offset to the payload
    ASSERT(off != 0) << "RDMA remote read key error: tab " << tableid << " key " << key;

    const uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

    RCQP *qp = get_qp(pid);

    RDMALockReq req(cor_id_);
    req.set_lock_meta(off,0,lock_content,data_ptr);
    req.set_read_meta(off + sizeof(uint64_t) + sizeof(uint64_t),data_ptr + sizeof(RdmaValHeader),len);

    req.post_reqs(scheduler_,qp);
    worker_->indirect_yield(yield);

    if(unlikely( *((uint64_t *)data_ptr) != 0)) {
      // lock fail
#if !NO_ABORT
      abort_ = true;
#endif
    }
    read_set_.emplace_back(tableid,key,(MemNode *)off,data_ptr + sizeof(RdmaValHeader),
                           0,
                           len,pid);
    return read_set_.size() - 1;
  }
#endif

  bool commit(yield_func_t &yield) {

    bool ret = true;
    if(abort_) {
      goto ABORT;
    }

    prepare_write_contents_f();

    log_remote(yield); // log remote using *logger_*
    //write_back_oneshot(yield);
    write_back_w_rdma(yield);
    gc_readset(); gc_writeset();
    return true;
 ABORT:
    gc_readset(); gc_writeset();
    return false;
  }

};


} // namespace rtx

} // namespace nocc
