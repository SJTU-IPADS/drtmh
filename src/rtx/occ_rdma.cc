#include "occ_rdma.h"
#include "rdma_req_helper.hpp"

namespace nocc {

namespace rtx {

bool OCCR::lock_writes_w_rdma(yield_func_t &yield) {

  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);
  RDMALockReq req(cor_id_);

  START(lock);
  // send requests
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) { // remote case
      auto off = (*it).off;

      // post RDMA requests
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

#if INLINE_OVERWRITE
      char *local_buf = (char *)((*it).data_ptr) - sizeof(MemNode);
#else
      // copy the seq out, since it will be overwritten by the remote op
      char *local_buf = (char *)((*it).data_ptr) - sizeof(RdmaValHeader);
      RdmaValHeader *h = (RdmaValHeader *)local_buf;
      it->seq = h->seq;
#endif

      req.set_lock_meta(off,0,lock_content,local_buf);
      req.set_read_meta(off + sizeof(uint64_t),local_buf + sizeof(uint64_t));

      req.post_reqs(scheduler_,qp);

      // two request need to be polled
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      }
      write_batch_helper_.mac_set_.insert(it->pid);
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
  END(lock);

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
      if(node->lock != 0){ // check locks
#if !NO_ABORT
        return false;
#endif
      }
      if(node->seq != (*it).seq) {     // check seqs
#if !NO_ABORT
        return false;
#endif
      }
    }
  }
  return true;
}


void OCCR::release_writes_w_rdma(yield_func_t &yield) {
  // can only work with lock_w_rdma
  uint64_t lock_content =  ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1);

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
      if(node->lock == 0) { // successfull locked
        //Qp *qp = qp_vec_[(*it).pid];
        Qp *qp = get_qp((*it).pid);
        assert(qp != NULL);
        node->lock = 0;
        scheduler_->post_send(qp,cor_id_,IBV_WR_RDMA_WRITE,(char *)(node),sizeof(uint64_t),
                              (*it).off,IBV_SEND_INLINE | IBV_SEND_SIGNALED);
      }
    } else {
      assert(false); // not implemented
    } // check pid
  }   // for
  worker_->indirect_yield(yield);
  return;
}

void OCCR::write_back_w_rdma(yield_func_t &yield) {

  /**
   * XD: it is harder to apply PA for one-sided operations.
   * This is because signaled requests are mixed with unsignaled requests.
   * It got little improvements, though. So I skip it now.
   */
  RDMAWriteReq req(cor_id_,PA /* whether to use passive ack*/);
  START(commit);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {

    if((*it).pid != node_id_) {

#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      node->seq = (*it).seq + 2; // update the seq
      node->lock = 0;            // re-set lock

      req.set_write_meta((*it).off + sizeof(RdmaValHeader),(*it).data_ptr,(*it).len);
      req.set_unlock_meta((*it).off);
      req.post_reqs(scheduler_,qp);

      // avoid send queue from overflow
      if(unlikely(qp->rc_need_poll())) {
        worker_->indirect_yield(yield);
      }

    } else { // local write
      inplace_write_op(it->node,it->data_ptr,it->len);
    } // check pid
  }   // for
  // gather results
  worker_->indirect_yield(yield);
  END(commit);
}

bool OCCR::validate_reads_w_rdma(yield_func_t &yield) {

  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).pid != node_id_) {

#if INLINE_OVERWRITE
      MemNode *node = (MemNode *)((*it).data_ptr - sizeof(MemNode));
#else
      RdmaValHeader *node = (RdmaValHeader *)((*it).data_ptr - sizeof(RdmaValHeader));
      it->seq = node->seq;
#endif
      //Qp *qp = qp_vec_[(*it).pid];
      Qp *qp = get_qp((*it).pid);
      assert(qp != NULL);

      scheduler_->post_send(qp,cor_id_,
                            IBV_WR_RDMA_READ,(char *)node,
                            sizeof(uint64_t) + sizeof(uint64_t), // lock + version
                            (*it).off,IBV_SEND_SIGNALED);


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
      if(node->seq != (*it).seq || node->lock != 0) { // check lock and versions
#if NO_ABORT
        return false;
#endif
      }
    }
  }
  return true;
}


/**
 * RPC handlers
 */
void OCCR::lock_rpc_handler2(int id,int cid,char *msg,void *arg) {

  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {
    auto item = (RtxLockItem *)ttptr;

    if(item->pid != response_node_)
      continue;

    MemNode *node = local_lookup_op(item->tableid,item->key);
    assert(node != NULL && node->value != NULL);
    RdmaValHeader *header = (RdmaValHeader *)(node->value);

    volatile uint64_t *lockptr = (volatile uint64_t *)header;
    if(unlikely( (*lockptr != 0) ||
                 !__sync_bool_compare_and_swap(lockptr,0,ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1)))) {
      res = LOCK_FAIL_MAGIC;
      break;
    }
    if(unlikely(header->seq != item->seq)) {
      res = LOCK_FAIL_MAGIC;
      break;
    }
  }

  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

void OCCR::release_rpc_handler2(int id,int cid,char *msg,void *arg) {
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

void OCCR::commit_rpc_handler2(int id,int cid,char *msg,void *arg) {

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

void OCCR::validate_rpc_handler2(int id,int cid,char *msg,void *arg) {

  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {

    ASSERT(num < 25) << "[Release RPC handler] lock " << num << " items.";

    auto item = (RtxLockItem *)ttptr;

    if(item->pid != response_node_)
      continue;

    auto node = local_lookup_op(item->tableid,item->key);
    RdmaValHeader *header = (RdmaValHeader *)(node->value);

    if(unlikely(item->seq != header->seq)) {
      res = LOCK_FAIL_MAGIC;
      break;
    }

  }
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}


} // namespace rtx

} // namespace nocc
