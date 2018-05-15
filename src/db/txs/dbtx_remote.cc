/* This file is used to implement OCC's remote operations */
/* OCC different from other concurrency control such that it is stateful, thus */

#include "dbtx.h"

//__thread std::map<uint64_t,DBTX::RemoteSet *> buffered_items_;
__thread std::map<uint64_t,DBTX*> *buffered_items_ = NULL;
__thread bool ro_helper_inited = false;

extern __thread RemoteHelper *remote_helper;

void RemoteHelper::thread_local_init() {

  if(false == ro_helper_inited) {
    buffered_items_ = new std::map<uint64_t ,DBTX *> ();
    ro_helper_inited = true;
    for(uint i = 0;i < total_servers_;++i) {
      for(uint j = 0;j < routines_ ;++j) {
        auto temp_tx = new DBTX(db_);
        temp_tx->init_temp();
        buffered_items_->insert(std::make_pair(_QP_ENCODE_ID(i,j + 1),temp_tx));
      }
    }
  }
}

/* implementation of remote helper */
RemoteHelper::RemoteHelper (MemDB *db,int servers,int cn)
  : db_(db), total_servers_(servers), routines_(cn) {

}

void RemoteHelper::begin(uint64_t id) {
#if 0
  thread_local_init();
  temp_ptr_ = new RemoteItemSet();
  buffered_items_->insert(std::make_pair(id,temp_ptr_));
#endif
  //  fprintf(stdout,"ro begin %d %d\n",_QP_DECODE_MAC(id),_QP_DECODE_INDEX(id));
  thread_local_init();
  assert(buffered_items_->find(id) != buffered_items_->end());
  temp_tx_ = (*buffered_items_)[id];
  temp_tx_->reset_temp();
  //temp_tx_->local_ro_begin();
}

void
RemoteHelper::add(MemNode *node,uint64_t seq) {
  //  temp_ptr_->add(node,seq);
}

uint16_t RemoteHelper::validate(uint64_t id) {
  assert(buffered_items_->find(id) != buffered_items_->end());
  temp_tx_ = (*buffered_items_)[id];
  //fprintf(stdout,"ro validate %d %d, %d\n",_QP_DECODE_MAC(id),_QP_DECODE_INDEX(id),temp_tx_->report_rw_set());
  //return temp_tx_->end_ro();
  if(temp_tx_->end_ro())
    return temp_tx_->report_rw_set() + 1;
  else {
    return 0;
  }
}

void DBTX::ro_val_rpc_handler(int id,int cid,char *msg,void *arg) {
  assert(cid != 0);
  char *reply_msg = rpc_->get_reply_buf();
  *((uint16_t *) reply_msg) = remote_helper->validate(_QP_ENCODE_ID(id,cid + 1));
  rpc_->send_reply(reply_msg,sizeof(uint16_t),id,thread_id,cid);
}
