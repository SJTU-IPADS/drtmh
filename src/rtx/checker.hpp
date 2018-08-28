#pragma once

#include "occ.h"

namespace nocc {

namespace rtx  {

class RdmaChecker {
 public:
  static bool check_lock_content(OCC *c,yield_func_t &yield) {

    char *lbuffer = (char *)Rmalloc(256);

    for(auto it = c->write_set_.begin();it != c->write_set_.end();++it) {
      if(it->pid != c->node_id_) {
        // send the RDMA request to check
        auto off = (*it).off;

        // post RDMA requests
        Qp *qp = c->qp_vec_[(*it).pid];
        assert(qp != NULL);

        c->scheduler_->post_send(qp,c->cor_id_,
                                 IBV_WR_RDMA_READ,lbuffer,sizeof(uint64_t) + sizeof(uint64_t),
                                 off,IBV_SEND_SIGNALED);
        INDIRECT_YIELD(yield);

        RdmaValHeader *h = (RdmaValHeader *)(it->data_ptr - sizeof(RdmaValHeader));
        RdmaValHeader *ch = (RdmaValHeader *)lbuffer;

        ASSERT(h->lock == 0) << " current lock content " << (uint64_t)(h->lock);
        ASSERT(h->seq == it->seq) << " header's seq " << h->seq << "; buffered seq " << it->seq;
        ASSERT(h->seq == ch->seq)  << " header's seq " << h->seq << "; checker's seq " << ch->seq;
        ASSERT(ch->lock == ENCODE_LOCK_CONTENT(c->response_node_,c->worker_id_,c->cor_id_ + 1))
            <<" check header's lock "<< ch->lock;
      }
    }
    LOG(2) << "lock content's check pass";
    Rfree(lbuffer);
    return true;
  }

  static bool check_log_content(OCC *c,yield_func_t &yield) {

    char *lbuffer = (char *)Rmalloc(4096);
    BatchOpCtrlBlock &b = c->write_batch_helper_;

    LOG(2) << "check log content, total log size " << (int)b.batch_msg_size()
           << ";rep factor "<< global_view->rep_factor_;

    if(c->write_set_.size() > 0 && global_view->rep_factor_ > 0) {

      std::set<int> backed_mac_set;
      for(auto it = c->write_set_.begin();
          it != c->write_set_.end();++it) {
        global_view->add_backup(it->pid,backed_mac_set);
      }

      auto &mem = c->logger_->mem_;
      for(auto it = backed_mac_set.begin();it != backed_mac_set.end();++it) {
        int m = *it;
        // get previous offset
        LOG(2) << "send read to " << m << " to check log";
        uint64_t off = mem.get_previous_remote_offset(c->node_id_,c->worker_id_,m,b.batch_msg_size());
        Qp *qp = c->qp_vec_[m];
        memset(lbuffer,0,4096);
        c->scheduler_->post_send(qp,c->cor_id_,
                                 IBV_WR_RDMA_READ,lbuffer,b.batch_msg_size(),
                                 off,IBV_SEND_SIGNALED);
        INDIRECT_YIELD(yield);

        // We use write_batch_helper's req_buf to post reqs.
        // So the remote log's content shall equal to local's.
        int n = memcmp(lbuffer, b.req_buf_, b.batch_msg_size());
        ASSERT(n == 0);
      }
    }
    LOG(2) << "check log content pass\n";
    Rfree(lbuffer);
    return true;
  }

  static bool check_backup_content(OCC *c,yield_func_t &yield) {

    // this buffer can be reused, since ReadItem is very small, so it can be inlined
    OCC::ReadItem *item = (OCC::ReadItem *)(c->rpc_->get_static_buf(sizeof(OCC::ReadItem)));

    for(auto it = c->write_set_.begin();it != c->write_set_.end();++it) {

      std::set<int> backed_mac_set;
      global_view->add_backup(it->pid,backed_mac_set);

      char reply_buf[1024];

      for(auto it1 = backed_mac_set.begin();it1 != backed_mac_set.end();++it1) {

        LOG(2) << "check " << "tab " << (int)(it->tableid) << ",key " << it->key
               << "; from primary " << (int)(it->pid) << " to back " << *it1;
        item->pid = it->pid;
        item->key = it->key;
        item->tableid = it->tableid;

        // send a RPC to request backup's key
        c->rpc_->prepare_multi_req(reply_buf,1,c->cor_id_);
        c->rpc_->append_req((char *)item,RTX_BACKUP_GET_ID,sizeof(OCC::ReadItem),c->cor_id_,
                            RRpc::REQ,*it1);
        INDIRECT_YIELD(yield);
        int n = memcmp(reply_buf,it->data_ptr,it->len);
        ASSERT(n == 0) << n;
      }
    }
    LOG(2) << "check backup's content pass\n";
    return true;
  }
};

} // namespace rtx

} // namespace nocc
