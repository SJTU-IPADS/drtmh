#include "tx_config.h" // some configurations
#include "rtx_occ.h"

namespace nocc {

namespace rtx {

RtxOCC::RtxOCC(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int cid,int response_node) :
    TXOpBase(worker,db,rpc_handler,response_node),
    read_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
    write_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
    cor_id_(cid),response_node_(nid)
{
  register_default_rpc_handlers();
  // resize the read/write set
  read_set_.reserve(12);
  write_set_.reserve(12);
}

void RtxOCC::begin(yield_func_t &yield) {

  abort_ = false;
  read_set_.clear();
  write_set_.clear();

  start_batch_read();
}

bool RtxOCC::commit(yield_func_t &yield) {
  // only execution phase
#if TX_ONLY_EXE
  gc_readset();
  gc_writeset();
  return true;
#endif

  bool ret = true;
  if(abort_) {
    goto ABORT;
  }

  // first, lock remote records
  ret = lock_writes(yield);
  if(unlikely(!ret)) {
    goto ABORT;
  }

  if(unlikely(!validate_reads(yield))) {
    goto ABORT;
  }

  prepare_write_contents();
  log_remote(yield); // log remote using *logger_*
  // write the modifications of records back
  write_back(yield);

  return true;
ABORT:
  release_writes(yield);
  return false;
}

int RtxOCC::local_read(int tableid,uint64_t key,int len,yield_func_t &yield) {

  char *temp_val = (char *)malloc(len);
  uint64_t seq;

  auto node = local_get_op(tableid,key,temp_val,len,seq);

  if(unlikely(node == NULL)) {
    free(temp_val);
    return -1;
  }
  // add to read-set
  int idx = read_set_.size();
  read_set_.emplace_back(tableid,key,node,temp_val,seq,len,node_id_);
  return idx;
}

int RtxOCC::local_insert(int tableid,uint64_t key,char *val,int len,yield_func_t &yield) {
  char *data_ptr = (char *)malloc(len);
  uint64_t seq;
  auto node = local_insert_op(tableid,key,seq);
  memcpy(data_ptr,val,len);
  write_set_.emplace_back(tableid,key,node,data_ptr,seq,len,node_id_);
  return write_set_.size() - 1;
}

int RtxOCC::remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
  return add_batch_read(tableid,key,pid,len);
}

int RtxOCC::remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
  return add_batch_insert(tableid,key,pid,len);
}

// helper function's impl

void RtxOCC::start_batch_read() {
  start_batch_rpc_op(read_batch_helper_);
}

// helper functions to add batch operations

int RtxOCC::add_batch_read(int tableid,uint64_t key,int pid,int len) {
  // add a batch read request
  int idx = read_set_.size();
  add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                               /* init RTXReadItem */ RTX_REQ_READ,pid,key,tableid,len,idx);
  read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
  return idx;
}


int RtxOCC::add_batch_insert(int tableid,uint64_t key,int pid,int len) {
  // add a batch read request
  int idx = read_set_.size();
  add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                               /* init RTXReadItem */ RTX_REQ_INSERT,pid,key,tableid,len,idx);
  read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
  return idx;
}

int RtxOCC::add_batch_write(int tableid,uint64_t key,int pid,int len) {
  // add a batch read request
  int idx = read_set_.size();
  add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                               /* init RTXReadItem */ RTX_REQ_READ_LOCK,pid,key,tableid,len,idx);
  read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
  return idx;
}


int RtxOCC::send_batch_read(int idx) {
  return send_batch_rpc_op(read_batch_helper_,cor_id_,RTX_READ_RPC_ID);
}

bool RtxOCC::parse_batch_result(int num) {

  char *ptr  = reply_buf_;
  for(uint i = 0;i < num;++i) {
    // parse a reply header
    RtxReplyHeader *header = (RtxReplyHeader *)(ptr);
    ptr += sizeof(RtxReplyHeader);
    for(uint j = 0;j < header->num;++j) {
      RtxOCCResponse *item = (RtxOCCResponse *)ptr;
      read_set_[item->idx].data_ptr = ptr + sizeof(RtxOCCResponse);
      read_set_[item->idx].seq      = item->seq;
      ptr += (sizeof(RtxOCCResponse) + item->payload);
    }
  }
  return true;
}

void RtxOCC::prepare_write_contents() {

  // Notice that it should contain local records
  // This function has to be called after lock + validation success
  write_batch_helper_.clear_buf(); // only clean buf, not the mac_set

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    //if((*it).pid != node_id_) {
    add_batch_entry_wo_mac<RtxWriteItem>(write_batch_helper_,
                                         (*it).pid,
                                         /* init write item */ (*it).pid,(*it).tableid,(*it).key,(*it).len);
    memcpy(write_batch_helper_.req_buf_end_,(*it).data_ptr,(*it).len);
    write_batch_helper_.req_buf_end_ += (*it).len;
    //}
  }
}

void RtxOCC::write_back(yield_func_t &yield) {

  // write back local records
  int written_items = 0;
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if(it->pid == node_id_) {
      inplace_write_op(it->node,it->data_ptr,it->len);
      written_items += 1;
    }
  }
  if(written_items == write_set_.size()) {// not remote records
#if !PA
    worker_->indirect_yield(yield);
#endif
    return;
  }
  // send the remote records
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_COMMIT_RPC_ID,PA);
#if PA == 0
  worker_->indirect_yield(yield);
#else
  write_batch_helper_.req_buf_ = rpc_->get_fly_buf(cor_id_); // update the buf to avoid on-flight overwrite
#endif
}

bool RtxOCC::release_writes(yield_func_t &yield) {
  start_batch_rpc_op(write_batch_helper_);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) { // remote case
      add_batch_entry<RtxLockItem>(write_batch_helper_, (*it).pid,
                                   /*init RTXLockItem */ (*it).pid,(*it).tableid,(*it).key,(*it).seq);
    }
    else {
      auto res = local_try_release_op(it->node,ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1));
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_RELEASE_RPC_ID);
  worker_->indirect_yield(yield);
}

void RtxOCC::log_remote(yield_func_t &yield) {

  if(write_set_.size() > 0 && view_->rep_factor_ > 0) {

    // re-use write_batch_helper_'s data structure
    BatchOpCtrlBlock cblock(write_batch_helper_.req_buf_,write_batch_helper_.reply_buf_);
    cblock.batch_size_  = write_batch_helper_.batch_size_;
    cblock.req_buf_end_ = write_batch_helper_.req_buf_end_;

#if EM_FASST
    view_->add_backup(response_node_,cblock.mac_set_);
#else
    for(auto it = write_batch_helper_.mac_set_.begin();
        it != write_batch_helper_.mac_set_.end();++it) {
      view_->add_backup(*it,cblock.mac_set_);
    }
    // add local server
    view_->add_backup(current_partition,cblock.mac_set_);
#endif

    logger_->log_remote(cblock,cor_id_);
    worker_->indirect_yield(yield);

#if 1
    cblock.req_buf_ = rpc_->get_fly_buf(cor_id_);
    memcpy(cblock.req_buf_,write_batch_helper_.req_buf_,write_batch_helper_.batch_msg_size());
    cblock.req_buf_end_ = cblock.req_buf_ + write_batch_helper_.batch_msg_size();
    //log ack
    logger_->log_ack(cblock,cor_id_); // need to yield
#endif
  } // end check whether it is necessary to log
}



bool RtxOCC::validate_reads(yield_func_t &yield) {

  start_batch_rpc_op(read_batch_helper_);
  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    if((*it).pid != node_id_) { // remote case
      add_batch_entry<RtxLockItem>(read_batch_helper_, (*it).pid,
                                   /*init RTXLockItem */ (*it).pid,(*it).tableid,(*it).key,(*it).seq);
    } else {
      if(!local_validate_op(it->node,it->seq)) {
#if !NO_ABORT
        return false;
#endif
      }
    }
  }
  send_batch_rpc_op(read_batch_helper_,cor_id_,RTX_VAL_RPC_ID);
  worker_->indirect_yield(yield);

  // parse the results
  for(uint i = 0;i < read_batch_helper_.mac_set_.size();++i) {
    if(*(get_batch_res<uint8_t>(read_batch_helper_,i)) == LOCK_FAIL_MAGIC) { // lock failed
      return false;
    }
  }
  return true;
}

bool RtxOCC::lock_writes(yield_func_t &yield) {

  start_batch_rpc_op(write_batch_helper_);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) { // remote case
      add_batch_entry<RtxLockItem>(write_batch_helper_, (*it).pid,
                                   /*init RTXLockItem */ (*it).pid,(*it).tableid,(*it).key,(*it).seq);
    }
    else {
      if(unlikely(!local_try_lock_op(it->node,
                                     ENCODE_LOCK_CONTENT(response_node_,worker_id_,cor_id_ + 1)))){
#if !NO_ABORT
        return false;
#endif
      }
      if(unlikely(!local_validate_op(it->node,it->seq))) {
#if !NO_ABORT
        return false;
#endif
      }
    }
  }
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_LOCK_RPC_ID);

  worker_->indirect_yield(yield);

  // parse the results
  for(uint i = 0;i < write_batch_helper_.mac_set_.size();++i) {
    if(*(get_batch_res<uint8_t>(write_batch_helper_,i)) == LOCK_FAIL_MAGIC) { // lock failed
      return false;
    }
  }
  return true;
}

/* RPC handlers */
void RtxOCC::read_rpc_handler(int id,int cid,char *msg,void *arg) {

  char* reply_msg = rpc_->get_reply_buf();
  char *reply = reply_msg + sizeof(RtxReplyHeader);
  int num_returned(0);

  RTX_ITER_ITEM(msg,sizeof(RTXReadItem)) {

    RTXReadItem *item = (RTXReadItem *)ttptr;

    if(item->pid != response_node_) {
      continue;
    }

    RtxOCCResponse *reply_item = (RtxOCCResponse *)reply;

    switch(item->type) {
      case RTX_REQ_READ: {
        // fetch the record
        uint64_t seq;
        auto node = local_get_op(item->tableid,item->key,reply + sizeof(RtxOCCResponse),item->len,seq);
        reply_item->seq = seq;
        reply_item->idx = item->idx;
        reply_item->payload = item->len;

        reply += (sizeof(RtxOCCResponse) + item->len);
      }
        break;
      case RTX_REQ_READ_LOCK: {
        uint64_t seq;
        MemNode *node = NULL;

        RtxOCCResponse *reply_item = (RtxOCCResponse *)reply;
        if(unlikely((node = local_try_lock_op(item->tableid,item->key,
                                              ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1))) == NULL)) {
          reply_item->seq = 0;
          reply_item->idx = item->idx;
          reply_item->payload = 0;
          reply += sizeof(RtxOCCResponse);
          break;
        } else {
          reply_item->seq = node->seq;
          reply_item->idx = item->idx;
          reply_item->payload = item->len;
          memcpy(reply + sizeof(RtxOCCResponse),node->padding,item->len);
          reply += (sizeof(RtxOCCResponse) + item->len);
        }
      }
        break;
      default:
        assert(false);
    }
    num_returned += 1;
  } // end for

  ((RtxReplyHeader *)reply_msg)->num = num_returned;
  assert(num_returned > 0);
  rpc_->send_reply(reply_msg,reply - reply_msg,id,cid);
  // send reply
}

void RtxOCC::lock_rpc_handler(int id,int cid,char *msg,void *arg) {

  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {
    auto item = (RtxLockItem *)ttptr;

    if(item->pid != response_node_)
      continue;

    MemNode *node = NULL;

    if(unlikely((node = local_try_lock_op(item->tableid,item->key,
                                          ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1))) == NULL)) {
      res = LOCK_FAIL_MAGIC;
      break;
    }
    if(unlikely(node->seq != item->seq)){
      res = LOCK_FAIL_MAGIC;
      break;
    }
  }

  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

void RtxOCC::release_rpc_handler(int id,int cid,char *msg,void *arg) {

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {
    auto item = (RtxLockItem *)ttptr;

    if(item->pid != response_node_)
      continue;
    auto res = local_try_release_op(item->tableid,item->key,
                                    ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1));
  }

  char* reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
}

void RtxOCC::commit_rpc_handler(int id,int cid,char *msg,void *arg) {

  RTX_ITER_ITEM(msg,sizeof(RtxWriteItem)) {

    auto item = (RtxWriteItem *)ttptr;
    ttptr += item->len;

    if(item->pid != response_node_) {
      continue;
    }
    inplace_write_op(item->tableid,item->key,  // find key
                     (char *)item + sizeof(RtxWriteItem),item->len);
  } // end for
#if PA == 0
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
#endif
}

void RtxOCC::validate_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {
    auto item = (RtxLockItem *)ttptr;

    if(item->pid != response_node_)
      continue;
    if(unlikely(!local_validate_op(item->tableid,item->key,item->seq))) {
      res = LOCK_FAIL_MAGIC;
      break;
    }
  }
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

void RtxOCC::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&RtxOCC::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&RtxOCC::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&RtxOCC::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  ROCC_BIND_STUB(rpc_,&RtxOCC::commit_rpc_handler,this,RTX_COMMIT_RPC_ID);
  ROCC_BIND_STUB(rpc_,&RtxOCC::validate_rpc_handler,this,RTX_VAL_RPC_ID);
}


}; // namespace rtx

};
