#pragma once

#include "rtx_occ.h"
#include "tx_config.h"

#include "core/logging.h"

namespace nocc {

namespace rtx {

// extend baseline RtxOCC with one-sided RDMA
class RtxOCCR : public RtxOCC {
public:
  RtxOCCR(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
          RdmaCtrl *cm,RDMA_sched* rdma_sched,int ms) :
      RtxOCC(worker,db,rpc_handler,nid,tid,cid,response_node,
             cm,rdma_sched,ms)
  {
    // register normal RPC handlers
    register_default_rpc_handlers();
  }

  // without cache's version
  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {

    char *data_ptr = (char *)Rmalloc(sizeof(MemNode) + len);
    uint64_t off = 0;
#if INLINE_OVERWRITE
    off = rdma_lookup_op(pid,tableid,key,data_ptr,yield);
#else
    off = rdma_read_val(pid,tableid,key,len,data_ptr,yield);
#endif
    MemNode *node = (MemNode *)data_ptr;
    ASSERT(off != 0) << "RDMA remote read key error: tab " << tableid << " key " << key;
    read_set_.emplace_back(tableid,key,(MemNode *)off,(char *)data_ptr + sizeof(MemNode),
                           node->seq, // seq shall be filled later
                           len,pid);
    return read_set_.size() - 1;
  }

  bool commit(yield_func_t &yield) {

#if TX_ONLY_EXE
    return dummy_commit();
#endif
    assert(false);
#if 1 //USE_RDMA_COMMIT
    if(!lock_writes_w_rdma(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#else
    if(!lock_writes(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#endif

#if 1 //USE_RDMA_COMMIT
    if(!validate_reads_w_rdma(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#else
    if(!validate_reads(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#endif
    return dummy_commit();

    prepare_write_contents();
    log_remote(yield); // log remote using *logger_*

#if 1 //USE_RDMA_COMMIT
    write_back_w_rdma(yield);
#else
    write_back(yield);
#endif

    gc_readset();
    gc_writeset();
    return true;
 ABORT:
#if 0 //USE_RDMA_COMMIT
    release_writes_w_rdma(yield);
#else
    release_writes(yield);
#endif
    gc_readset();
    gc_writeset();
    return false;
  }

  bool lock_writes_w_rdma(yield_func_t &yield) {

    uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

    // send requests
    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if((*it).pid != node_id_) { // remote case
        auto off = (*it).off;

        // post RDMA requests
        Qp *qp = qp_vec_[(*it).pid];
        assert(qp != NULL);

        // MemNode : lock | seq | ...
        qp->rc_post_compare_and_swap((char *)((*it).data_ptr) - sizeof(MemNode),off,0,
                                     lock_content,0 /* flag */ ,cor_id_);
        // read the seq
        qp->rc_post_send(IBV_WR_RDMA_READ,(char *)((*it).data_ptr - sizeof(MemNode)) + sizeof(uint64_t), // the second position store the seq
                         sizeof(uint64_t),off + sizeof(uint64_t),IBV_SEND_SIGNALED,cor_id_);
        scheduler_->add_pending(cor_id_,qp);
      }
      else {
        if(unlikely(!local_try_lock_op(it->node,
                                       ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
#if !NO_ABORT
          return false;
#endif
        } // check local lock
        if(unlikely(!local_validate_op(it->node,it->seq))) {
#if !NO_ABORT
          return false;
#endif
        } // check seq
      }
    } // end for

    worker_->indirect_yield(yield);
    // gather replies

    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if((*it).pid != node_id_) {
        MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
        if(node->lock != 0){ // check locks
          return false;
        }
        if(node->seq != (*it).seq) {     // check seqs
          return false;
        }
      }
    }
    return true;
  }

  void release_writes_w_rdma(yield_func_t &yield) {
    // can only work with lock_w_rdma
    uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if((*it).pid != node_id_) {
        MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
        if(node->lock == 0) { // successfull locked
          Qp *qp = qp_vec_[(*it).pid];
          assert(qp != NULL);
          node->lock = 0;
          qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)(node),sizeof(uint64_t),
                           (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED,cor_id_);
          scheduler_->add_pending(cor_id_,qp);
        }
      } else {
        assert(false); // not implemented
      } // check pid
    }   // for
    worker_->indirect_yield(yield);
    return;
  }

  void write_back_w_rdma(yield_func_t &yield) {

    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if((*it).pid != node_id_) {
        MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
        Qp *qp = qp_vec_[(*it).pid];
        assert(qp != NULL);
        node->seq = (*it).seq + 2; // update the seq
        node->lock = 0;            // re-set lock

        int flag = IBV_SEND_INLINE; // flag used for RDMA write
#if !PA
        flag |= IBV_SEND_SIGNALED;
#endif

#if INLINE_OVERWRITE  // value in index
        // node { | lock | other fields |}
        // write contents back
        qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)node + sizeof(uint64_t), sizeof(MemNode) - sizeof(uint64_t),
                         (*it).off + sizeof(uint64_t), // omit the first lock
                         0 /* flag*/,cor_id_);
        // release the lock
        qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)node,sizeof(uint64_t),(*it).off,
                         flag,cor_id_);

#else   // the case with value out index
        qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)node + sizeof(MemNode),(*it).len,
                         node->off,
                         0 /* flag*/,cor_id_);
        qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)node,sizeof(uint64_t) + sizeof(uint64_t),(*it).off,
                         flag,cor_id_);
#endif
#if !PA // if not use passive ack, add qp to the pending list
        scheduler_->add_pending(cor_id_,qp);
#endif
      } else { // local write
        inplace_write_op(it->node,it->data_ptr,it->len);
      } // check pid
    }   // for
    // gather results
#if !PA
    worker_->indirect_yield(yield);
#endif
  }

  bool validate_reads_w_rdma(yield_func_t &yield) {
    for(auto it = read_set_.begin();it != read_set_.end();++it) {
      if((*it).pid != node_id_) {
        MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
        Qp *qp = qp_vec_[(*it).pid];
        assert(qp != NULL);

        qp->rc_post_send(IBV_WR_RDMA_READ,(char *)node,
                         sizeof(uint64_t) + sizeof(uint64_t), // lock + version
                         (*it).off,IBV_SEND_SIGNALED,cor_id_);
        scheduler_->add_pending(cor_id_,qp);
      } else { // local case
        if(!local_validate_op(it->node,it->seq)) {
#if !NO_ABORT
          return false;
#endif
        }
      }
    }

    worker_->indirect_yield(yield);

    for(auto it = read_set_.begin();it != read_set_.end();++it) {
      if((*it).pid != node_id_) {
        MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
        if(node->seq != (*it).seq || node->lock != 0) // check lock and versions
          return false;
      }
    }
    return true;
  }

  // overwrite GC functions, to use Rfree
  void gc_readset() {
    for(auto it = read_set_.begin();it != read_set_.end();++it) {
      // the first part of data_ptr is reserved to store MemNode
      if(it->pid != node_id_)
        Rfree((*it).data_ptr - sizeof(MemNode));
      else
        free((*it).data_ptr);
    }
  }

  void gc_writeset() {
    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      // the first part of data_ptr is reserved to store MemNode
      if(it->pid != node_id_)
        Rfree((*it).data_ptr - sizeof(MemNode));
      else
        free((*it).data_ptr);
    }
  }

  bool dummy_commit() {
    // clean remaining resources
    gc_readset();
    gc_writeset();
    return true;
  }

};

} // namespace rtx
} // namespace nocc
