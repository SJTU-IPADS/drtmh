#pragma once

#include "rdmaio.h"

namespace nocc {

namespace rtx {

/**
 * A simple wrapper to help set the meta data
 */
class RDMAReqBase {
 protected:
  explicit RDMAReqBase(int cid) {
    // fill the reqs with initial value
    sr[0].num_sge = 1; sr[0].sg_list = &sge[0];
    sr[1].num_sge = 1; sr[1].sg_list = &sge[1];

    // coroutine id
    sr[0].send_flags = 0;
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[1].wr_id = RDMA_sched::encode_wrid(cid,1);

    sr[0].next = &sr[1];
    sr[1].next = NULL;
  }

  struct ibv_send_wr sr[2];
  struct ibv_sge     sge[2];
  struct ibv_send_wr *bad_sr;
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

  inline void post_reqs(Qp *qp) {

    sr[0].wr.atomic.remote_addr += qp->remote_attr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_attr_.rkey;
    sge[0].lkey = qp->dev_->conn_buf_mr->lkey;

    sr[1].wr.rdma.remote_addr += qp->remote_attr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_attr_.rkey;
    sge[1].lkey = qp->dev_->conn_buf_mr->lkey;

    qp->rc_post_batch(&(sr[0]),&bad_sr,1);
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
  explicit RDMAWriteReq(int cid) : RDMAReqBase(cid) {
    sr[0].opcode = IBV_WR_RDMA_WRITE;
    sr[1].opcode = IBV_WR_RDMA_WRITE;
  }

};



} // namespace rtx

} // namespace nocc
