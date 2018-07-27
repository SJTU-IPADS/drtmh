#pragma once

#include "rtx_occ.h"
#include "tx_config.h"

#include "core/logging.h"

namespace nocc {

namespace rtx {

/**
 * New meta data for each record
 */
struct RdmaValHeader {
  uint64_t lock;
  uint64_t seq;
};

/**
 * Extend baseline RtxOCC with one-sided RDMA support for execution, validation and commit.
 */
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

    char *data_ptr = (char *)Rmalloc(sizeof(MemNode) + len + sizeof(RdmaValHeader));
    ASSERT(data_ptr != NULL);

    uint64_t off = 0;
#if INLINE_OVERWRITE
    off = rdma_lookup_op(pid,tableid,key,data_ptr,yield);
    MemNode *node = (MemNode *)data_ptr;
    auto seq = node->seq;
    data_ptr = data_ptr + sizeof(MemNode);
#else
    off = rdma_read_val(pid,tableid,key,len,data_ptr,yield,sizeof(RdmaValHeader));
    RdmaValHeader *header = (RdmaValHeader *)data_ptr;
    auto seq = header->seq;
    data_ptr = data_ptr + sizeof(RdmaValHeader);
#endif
    ASSERT(off != 0) << "RDMA remote read key error: tab " << tableid << " key " << key;

    read_set_.emplace_back(tableid,key,(MemNode *)off,data_ptr,
                           seq,
                           len,pid);
    return read_set_.size() - 1;
  }

  bool commit(yield_func_t &yield) {

#if TX_ONLY_EXE
    return dummy_commit();
#endif

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

#if 0 //USE_RDMA_COMMIT
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

  /**
   * We encode raw RDMA structure here.
   * The lock requests contains two consecutive requests to one QP,
   * one for lock and one for validate.
   */
  struct RDMALockReq {

    struct ibv_send_wr sr[2];
    struct ibv_send_wr *bad_sr;
    struct ibv_sge     sge[2];

    explicit RDMALockReq(int cid) {
      // op code
      sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
      sr[1].opcode = IBV_WR_RDMA_READ;

      // sge
      sr[0].num_sge = 1; sr[0].sg_list = &sge[0];
      sr[1].num_sge = 1; sr[1].sg_list = &sge[1];

      // coroutine id
      sr[0].send_flags = 0;
      sr[0].wr_id = cid;
      sr[1].send_flags = IBV_SEND_SIGNALED;
      sr[1].wr_id = cid;

      // meta data
      sge[0].length = sizeof(uint64_t);
      sge[1].length = sizeof(uint64_t);

      // link together
      sr[0].next = &sr[1];
      sr[1].next = NULL;
    }

    inline void set_lock_meta(Qp *qp,uint64_t remote_off,uint64_t compare, uint64_t swap,
                         char *local_addr) {
      sr[0].wr.atomic.remote_addr = remote_off + qp->remote_attr_.buf;
      sr[0].wr.atomic.compare_add = compare;
      sr[0].wr.atomic.swap = swap;
      sr[0].wr.atomic.rkey = qp->remote_attr_.rkey;

      sge[0].addr = (uint64_t)local_addr;
      sge[0].lkey = qp->dev_->conn_buf_mr->lkey;
    }

    inline void set_read_meta(Qp *qp,uint64_t remote_off,char *local_addr) {
      sr[1].wr.rdma.remote_addr =  remote_off + qp->remote_attr_.buf;
      sr[1].wr.rdma.rkey = qp->remote_attr_.rkey;

      sge[1].addr = (uint64_t)local_addr;
      sge[1].lkey = qp->dev_->conn_buf_mr->lkey;
    }
  };

  bool lock_writes_w_rdma(yield_func_t &yield) {

    uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);
    RDMALockReq req(cor_id_);

    // send requests
    for(auto it = write_set_.begin();it != write_set_.end();++it) {
      if((*it).pid != node_id_) { // remote case
        auto off = (*it).off;

        // post RDMA requests
        Qp *qp = qp_vec_[(*it).pid];
        assert(qp != NULL);

        char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);

        req.set_lock_meta(qp,off,0,lock_content,local_buf);
        req.set_read_meta(qp,off + sizeof(uint64_t),local_buf + sizeof(uint64_t));

        qp->rc_post_batch(&(req.sr[0]),&(req.bad_sr));
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

  /**
   * GC the read/write set is a little complex using RDMA.
   * Since some pointers are allocated from the RDMA heap, not from local heap.
   */
  void gc_helper(std::vector<ReadSetItem> &set) {
    for(auto it = set.begin();it != set.end();++it) {
      if(it->pid != node_id_) {
#if INLINE_OVERWRITE
        Rfree((*it).data_ptr - sizeof(MemNode));
#else
        Rfree((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      }
      else
        free((*it).data_ptr);

    }
  }

  // overwrite GC functions, to use Rfree
  void gc_readset() {
    gc_helper(read_set_);
  }

  void gc_writeset() {
    gc_helper(write_set_);
  }

  bool dummy_commit() {
    // clean remaining resources
    gc_readset();
    gc_writeset();
    return true;
  }

  /**
   * A specific lock handler, use the meta data encoded in value
   * This is because we only cache one address, so it's not so easy to
   * to encode meta data in the index (so that we need to cache the index).
   */
  void lock_rpc_handler2(int id,int cid,char *msg,void *arg) {

    char* reply_msg = rpc_->get_reply_buf();
    uint8_t res = LOCK_SUCCESS_MAGIC; // success

    RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {
      auto item = (RtxLockItem *)ttptr;

      if(item->pid != response_node_)
        continue;

      MemNode *node = local_lookup_op(item->tableid,item->key);
      assert(node != NULL && node->value != NULL);
      RdmaValHeader *header = (RdmaValHeader *)(node->value);

      volatile uint64_t *lockptr = (volatile uint64_t *)lockptr;
      if( unlikely( (*lockptr != 0) ||
                    !__sync_bool_compare_and_swap(lockptr,0,ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1)))) {
        res = LOCK_FAIL_MAGIC;
        break;
      }
      if(unlikely(node->seq != item->seq)) {
        res = LOCK_FAIL_MAGIC;
        break;
      }
    }

    *((uint8_t *)reply_msg) = res;
    rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
  }

  void release_rpc_handler2(int id,int cid,char *msg,void *arg) {
    RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {
      auto item = (RtxLockItem *)ttptr;

      if(item->pid != response_node_)
        continue;
      auto node = local_lookup_op(item->tableid,item->key);
      assert(node != NULL && node->value != NULL);

      RdmaValHeader *header = (RdmaValHeader *)(node->value);
      volatile uint64_t *lockptr = (volatile uint64_t *)lockptr;
      !__sync_bool_compare_and_swap(lockptr,ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1),0);
    }

    char* reply_msg = rpc_->get_reply_buf();
    rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
  }

  void commit_rpc_handler2(int id,int cid,char *msg,void *arg) {
    RTX_ITER_ITEM(msg,sizeof(RtxWriteItem)) {

      auto item = (RtxWriteItem *)ttptr;
      ttptr += item->len;

      if(item->pid != response_node_) {
        continue;
      }
      auto node = inplace_write_op(item->tableid,item->key,  // find key
                                   (char *)item + sizeof(RtxWriteItem),item->len);
      RdmaValHeader *header = (RdmaValHeader *)(node->value);
      header->seq += 2;
      asm volatile("" ::: "memory");
      header->lock = 0;
    } // end for
#if PA == 0
    char *reply_msg = rpc_->get_reply_buf();
    rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
#endif
  }
};

} // namespace rtx
} // namespace nocc
