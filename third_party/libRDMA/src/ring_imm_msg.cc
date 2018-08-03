#include "ring_imm_msg.h"
#include "utils.h"
#include "ralloc.h"

#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>

#define FORCE 0 //Whether to force poll completion
#define FORCE_REPLY 0

#define USE_SEND 0

namespace rdmaio {

  namespace ring_imm_msg {

#define SET_HEADER_TAILER(msgp,len) \
    *((uint64_t *)msgp) = len; \
    *((uint64_t *)(msgp + sizeof(uint64_t) + len)) = len;\
    len += sizeof(uint64_t) * 2;\

    RingMessage::RingMessage(uint64_t ring_size,uint64_t ring_padding,
                             int thread_id,RdmaCtrl *cm,char *base_ptr,msg_func_t func)
      :ring_size_(ring_size),
       ring_padding_(ring_padding),
       thread_id_(thread_id),
       num_nodes_(cm->get_num_nodes()),
       total_buf_size_(ring_size + ring_padding),
       cm_(cm),
       max_recv_num_(MAX_RECV_SIZE),
       node_id_(cm->get_nodeid()),
       callback_(func)
    {
#if FORCE == 1
      // warning!
#endif
      //fprintf(stdout,"ring message @ %d init\n",thread_id);
      base_ptr_ = (base_ptr + (total_buf_size_ + MSG_META_SZ) * num_nodes_ * thread_id);

      for(uint i = 0;i < num_nodes_;++i) {
        offsets_[i] = 0;
        headers_[i] = 0;
      }

      /* do we really need to clean the buffer here ? */
      //  memset(base_ptr_,0, (total_buf_size_ + MSG_META_SZ) * num_nodes_ );

      char *start_ptr = (char *)(cm_->conn_buf_);

      /* The baseoffsets must be synchronized at all machine */
      base_offset_ = base_ptr_ - start_ptr;

      // calculate the recv_buf_size
      recv_buf_size_ = 0;
      while(recv_buf_size_ < MAX_PACKET_SIZE){
        recv_buf_size_ += MIN_STEP_SIZE;
      }

      // init qp vector
      for(uint i = 0;i < num_nodes_;++i) {
        Qp *qp = cm_->get_rc_qp(thread_id,i,0);
        assert(qp != NULL);
        qp_vec_.push_back(qp);
        init(i);
      }
    }

    Qp::IOStatus
    RingMessage::send_to(int node,char *msgp,int len) {

      //SET_HEADER_TAILER(msgp,len);
      int ret = (int) Qp::IO_SUCC;

      // calculate offset
      uint64_t offset = base_offset_ + node_id_ * (total_buf_size_ + MSG_META_SZ) +
        (offsets_[node] % ring_size_) + MSG_META_SZ;
      // printf("send_offset: %lu to: %d, r_off:%lu\n", node_id_ * (total_buf_size_ + MSG_META_SZ) +
      //   (offsets_[node] % ring_size_), node, offset);
      offsets_[node] += len;
      
      assert(len <= ring_padding_);

      // get qp
      Qp *qp = qp_vec_[node];

      // calculate send flag
      int send_flag = (len < 64) ? (IBV_SEND_INLINE) : 0;

#if FORCE_REPLY == 1
      send_flag |= IBV_SEND_SIGNALED;
#else
      if(qp->first_send()) {
        send_flag |= IBV_SEND_SIGNALED;
      }

      if(qp->need_poll())
        ret |= qp->poll_completion();
#endif

      // post the request
      // printf("write to-> %p! total_size %u \n", qp->remote_attr_.buf + offset, len);
      ibv_wr_opcode op;
#if USE_SEND == 1
      op = IBV_WR_SEND_WITH_IMM;
#else
      op = IBV_WR_RDMA_WRITE_WITH_IMM;
#endif

      ImmMeta meta;
      meta.nid  = node_id_;
      meta.size = len;
      meta.cid = 0;
      
      ret |= qp->rc_post_send(op,msgp,len, offset,send_flag,0,meta.content);

#if FORCE_REPLY == 1
      ret |= qp->poll_completion();
#endif
      assert(ret == Qp::IO_SUCC);
      return (Qp::IOStatus)ret;
    }

    Qp::IOStatus
    RingMessage::broadcast_to(int *nodeids, int num, char *msg,int len) {

      //SET_HEADER_TAILER(msg,len);

      int ret = (int)(Qp::IO_SUCC);

      /* maybe we shall avoid this?*/
      uint64_t remote_offsets[MAX_BROADCAST_SERVERS];

      for(uint i = 0;i < num;++i) {
        // calculate the offset
        //fprintf(stdout,"start send to  @%d\n",thread_id_);
        uint64_t off = base_offset_ +  node_id_ * (total_buf_size_ + MSG_META_SZ) +
          offsets_[nodeids[i]] % ring_size_ + MSG_META_SZ;
        // printf("send_offset_broad : %lu to: %d, r_off:%lu\n", node_id_ * (total_buf_size_ + MSG_META_SZ) +
        //   (offsets_[nodeids[i]] % ring_size_), nodeids[i], off);
        offsets_[nodeids[i]] += len;

        //fprintf(stdout,"start send to %d, off %lu @%d\n",nodeids[i],off,thread_id_);

        remote_offsets[i] = off;
        
      }


      for(uint i = 0;i < num;++i) {

        Qp *qp = qp_vec_[nodeids[i]];

        int send_flag = 0;
#if FORCE == 1
        send_flag = IBV_SEND_SIGNALED;
#else
        if(qp->first_send()) {
          send_flag = IBV_SEND_SIGNALED;
        }
        if(qp->need_poll()) {
          Qp::IOStatus s = qp->poll_completion();
          ret |= (int)s;
        }
#endif
        ibv_wr_opcode op;
#if USE_SEND == 1
        op = IBV_WR_SEND_WITH_IMM;
#else
        op = IBV_WR_RDMA_WRITE_WITH_IMM;
#endif

        ImmMeta meta;
        meta.nid  = node_id_;
        meta.size = len;
        meta.cid = 0;

        ret |= qp->rc_post_send(op,msg,len,remote_offsets[i],send_flag,0,meta.content);

#if FORCE == 1
        Qp::IOStatus s = qp->poll_completion();
#endif
        ///ret |= (int)s;
        assert(ret == Qp::IO_SUCC);
      }
      return (Qp::IOStatus)ret;
    }

    void RingMessage::force_sync(int *node_ids, int num_of_node) {
      for(uint i = 0;i < num_of_node;++i) {
        qp_vec_[node_ids[i]]->force_poll();
      }
    }

    bool
    RingMessage::try_recv_from(int from_mac, char *buffer) {
      assert(false);
    }


    inline char *
    RingMessage::try_recv_from(int from_mac) {
      uint64_t poll_offset = from_mac * (total_buf_size_ + MSG_META_SZ) + headers_[from_mac] % ring_size_;
      return base_ptr_ + poll_offset + MSG_META_SZ;
    }

    void RingMessage::check() { }

    void RingMessage::poll_comps() {
      // assert(false);
      // for(uint nid = 0; nid < num_nodes_; ++nid) {
        int poll_result = ibv_poll_cq(qp_vec_[0]->recv_cq, RC_MAX_SHARED_RECV_SIZE,wc_);

        // prepare for replies
        assert(poll_result >= 0); // FIXME: ignore error

        for(uint i = 0;i < poll_result;++i) {
          // msg_num: poll_result

          // if(wc_[i].status != IBV_WC_SUCCESS) assert(false); // FIXME!
          if (wc_[i].status != IBV_WC_SUCCESS) {
            fprintf (stderr,
                 "got bad completion with status: 0x%x, vendor syndrome: 0x%x, with error %s\n",
                 wc_[i].status, wc_[i].vendor_err,ibv_wc_status_str(wc_[i].status));
            assert(false);
          }

          ImmMeta meta;
          meta.content = wc_[i].imm_data;
          uint32_t nid = meta.nid;
          char* msg;
#if USE_SEND == 1
          msg = (char*)wc_[i].wr_id;
#else
          msg = try_recv_from(nid);
#endif
          callback_(msg, nid,thread_id_);
#if USE_SEND == 0
          ack_msg(nid, meta.size);
#endif
          idle_recv_nums_[nid] += 1;
          if(idle_recv_nums_[nid] > max_idle_recv_num_) {
            // printf("--posted :%d\n",idle_recv_nums_[nid]);
            post_recvs(idle_recv_nums_[nid], nid);
            idle_recv_nums_[nid] = 0;
          }
        }
    }


    void RingMessage::init(uint32_t nid) {

      struct ibv_recv_wr* rr = rrs_[nid];
      struct ibv_sge* sge = sges_[nid];

      // uintptr_t ptr = (uintptr_t)Rmalloc(recv_buf_size_);

      // init recv relate data structures
      for(int i = 0; i < max_recv_num_; i++) {
#if USE_SEND == 1
        sge[i].length = recv_buf_size_;
        sge[i].lkey   = qp_vec_[nid]->dev_->conn_buf_mr->lkey;
        sge[i].addr   = (uintptr_t)Rmalloc(recv_buf_size_);
        assert(sge[i].addr != 0);
#else
        sge[i].length = 0;
        sge[i].lkey   = qp_vec_[nid]->dev_->conn_buf_mr->lkey;
        sge[i].addr   = (uintptr_t)NULL;
#endif
        rr[i].wr_id   = sge[i].addr;
        rr[i].sg_list = &sge[i];
        rr[i].num_sge = 1;

        rr[i].next    = (i < max_recv_num_ - 1) ?
          &rr[i + 1] : &rr[0];
      }

      idle_recv_nums_[nid] = 0;
      recv_heads_[nid] = 0;

      // post these recvs
      post_recvs(max_recv_num_, nid);
    }

    inline void RingMessage::post_recvs(int recv_num, uint32_t nid) {
      int tail   = recv_heads_[nid] + recv_num - 1;
      if(tail >= max_recv_num_) {
        tail -= max_recv_num_;
      }
      ibv_recv_wr  *head_rr = rrs_[nid] + recv_heads_[nid];
      ibv_recv_wr  *tail_rr = rrs_[nid] + tail;

      ibv_recv_wr  *temp = tail_rr->next;
      tail_rr->next = NULL;

      int rc = ibv_post_recv(qp_vec_[nid]->qp,head_rr,&bad_rr_);
      CE_1(rc, "[RingMessage] qp: Failed to post_recvs, %s\n", strerror(errno));

      tail_rr->next = temp; // restore the recv chain
      recv_heads_[nid] = (tail + 1) % max_recv_num_;

    }


    // end namespace msg
  }
};
