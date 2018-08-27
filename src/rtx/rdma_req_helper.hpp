#pragma once

#include "core/rdma_sched.h"
#include "tx_config.h"

namespace nocc {

namespace rtx {

/**
 * A simple wrapper to help set the meta data
 * One-sided reqs which are batched together cannot be simply written with using libRDMA's API.
 * Fortunately, most of the doorbelled requests used in transactions finish with exactly 2 hop.
 * So we mannaly set the sr and sges to post these requests.
 */
class RDMAReqBase {
 protected:
  explicit RDMAReqBase(int cid) : cor_id(cid) {
    // fill the reqs with initial value
    sr[0].num_sge = 1; sr[0].sg_list = &sge[0];
    sr[1].num_sge = 1; sr[1].sg_list = &sge[1];

    // coroutine id
    sr[0].send_flags = 0;
    sr[1].send_flags = IBV_SEND_SIGNALED;

    sr[0].next = &sr[1];
    sr[1].next = NULL;
  }

  struct ibv_send_wr sr[2];
  struct ibv_sge     sge[2];
  struct ibv_send_wr *bad_sr;
  int cor_id;
};

/**
 * Raw RDMA req to help issue *lock* requests to a record.
 * Here, we assume that:
 *  - *CAS* operation, which implements try lock; and
 *  - *READ* operation, which implements validation;
 * are batched using doorbell batching to the same node.
 */
class RDMALockReq  : public RDMAReqBase {
 public:
  explicit RDMALockReq(int cid) : RDMAReqBase(cid)
  {
    // op code
    sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr[1].opcode = IBV_WR_RDMA_READ;
  }

  inline void set_lock_meta(uint64_t remote_off,uint64_t compare, uint64_t swap,
                            char *local_addr) {
    sr[0].wr.atomic.remote_addr = remote_off;
    sr[0].wr.atomic.compare_add = compare;
    sr[0].wr.atomic.swap = swap;
    sge[0].length = sizeof(uint64_t);
    sge[0].addr = (uint64_t)local_addr;
  }

  inline void set_read_meta(uint64_t remote_off,char *local_addr) {
    sr[1].wr.rdma.remote_addr =  remote_off;
    sge[1].addr = (uint64_t)local_addr;
    sge[1].length = sizeof(uint64_t);
  }

  inline void post_reqs(oltp::RScheduler *s,Qp *qp) {

    sr[0].wr.atomic.remote_addr += qp->remote_attr_.memory_attr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_attr_.memory_attr_.rkey;
    sge[0].lkey = qp->dev_->conn_buf_mr->lkey;

    sr[1].wr.rdma.remote_addr += qp->remote_attr_.memory_attr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_attr_.memory_attr_.rkey;
    sge[1].lkey = qp->dev_->conn_buf_mr->lkey;

    s->post_batch(qp,cor_id,&(sr[0]),&bad_sr,1);
  }
};


/**
 * Raw RDMA req to help issue *commit* requests to a record.
 * Here, we assume that:
 *  - *WRITE* operation, which implements write-back; and
 *  - *WRITE* operation, which implements unlock.
 * are batched using doorbell batching to the same node.
 */
class RDMAWriteReq : RDMAReqBase {
 public:
  RDMAWriteReq(int cid,bool pa) : RDMAReqBase(cid),pa(pa)
  {
    sr[0].opcode = IBV_WR_RDMA_WRITE;
    sr[1].opcode = IBV_WR_RDMA_WRITE;

    // clear the flags if passive ACK is used
    if(pa) {
      sr[1].send_flags = 0;
    }
    sr[1].send_flags |= IBV_SEND_INLINE;
  }

  inline void set_write_meta(uint64_t remote_off,char *local_addr,int size) {
    sr[0].wr.rdma.remote_addr =  remote_off;
    sge[0].addr = (uint64_t)local_addr;
    sge[0].length = size;
    if(size < 64) {
      sr[0].send_flags |= IBV_SEND_INLINE;
    }
  }

  inline void set_unlock_meta(uint64_t remote_off) {
    assert(dummy == 0);
    sr[1].wr.rdma.remote_addr =  remote_off;
    sge[1].addr = (uint64_t)(&dummy);
    sge[1].length = sizeof(uint64_t);
  }

  inline void post_reqs(oltp::RScheduler *s,Qp *qp) {

    sr[0].wr.rdma.remote_addr += qp->remote_attr_.memory_attr_.buf;
    sr[0].wr.rdma.rkey = qp->remote_attr_.memory_attr_.rkey;
    sge[0].lkey = qp->dev_->conn_buf_mr->lkey;

    sr[1].wr.rdma.remote_addr += qp->remote_attr_.memory_attr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_attr_.memory_attr_.rkey;
    sge[1].lkey = qp->dev_->conn_buf_mr->lkey;

    if(!pa) {
      s->post_batch(qp,cor_id,&(sr[0]),&bad_sr,1);
    } else {
      s->post_batch_pending(qp,cor_id,&(sr[0]),&bad_sr,1);
    }
  }
 private:
  uint64_t dummy = 0;
  bool pa;
};



} // namespace rtx

} // namespace nocc
