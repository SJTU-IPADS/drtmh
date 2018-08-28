#include "tx_config.h" // some configurations
#include "occ.h"

#include "global_vars.h"

#include "util/mapped_log.h"

namespace nocc {

extern __thread MappedLog local_log;

namespace rtx {

OCC::OCC(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int cid,int response_node) :
    TXOpBase(worker,db,rpc_handler,response_node),
    read_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
    write_batch_helper_(rpc_->get_static_buf(MAX_MSG_SIZE),reply_buf_),
    read_set_(),
    write_set_(),
    cor_id_(cid),response_node_(nid)
{
  if(worker_id_ == 0 && cor_id_ == 0)
    LOG(3) << "Baseline OCC.";
  register_default_rpc_handlers();
  memset(reply_buf_,0,MAX_MSG_SIZE);

  // resize the read/write set
  read_set_.reserve(12);
  write_set_.reserve(12);
}

void OCC::begin(yield_func_t &yield) {

  abort_ = false;
  read_set_.clear();
  write_set_.clear();

  start_batch_read();
}

bool OCC::commit(yield_func_t &yield) {
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
  write_back_oneshot(yield);
  return true;
ABORT:
  release_writes(yield);
  return false;
}

int OCC::local_read(int tableid,uint64_t key,int len,yield_func_t &yield) {

  char *temp_val = (char *)malloc(len);
  uint64_t seq;

  auto node = local_get_op(tableid,key,temp_val,len,seq,db_->_schemas[tableid].meta_len);

  if(unlikely(node == NULL)) {
    free(temp_val);
    return -1;
  }
  // add to read-set
  int idx = read_set_.size();
  read_set_.emplace_back(tableid,key,node,temp_val,seq,len,node_id_);
  return idx;
}

int OCC::local_insert(int tableid,uint64_t key,char *val,int len,yield_func_t &yield) {
  char *data_ptr = (char *)malloc(len);
  uint64_t seq;
  auto node = local_insert_op(tableid,key,seq);
  memcpy(data_ptr,val,len);
  write_set_.emplace_back(tableid,key,node,data_ptr,seq,len,node_id_);
  return write_set_.size() - 1;
}

int OCC::remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
  return add_batch_read(tableid,key,pid,len);
}

int OCC::remote_insert(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {
  return add_batch_insert(tableid,key,pid,len);
}

// helper function's impl

void OCC::start_batch_read() {
  start_batch_rpc_op(read_batch_helper_);
}

// helper functions to add batch operations

int OCC::add_batch_read(int tableid,uint64_t key,int pid,int len) {
  // add a batch read request
  int idx = read_set_.size();
  add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                               /* init RTXReadItem */ RTX_REQ_READ,pid,key,tableid,len,idx);
  read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
  return idx;
}


int OCC::add_batch_insert(int tableid,uint64_t key,int pid,int len) {
  // add a batch read request
  int idx = read_set_.size();
  add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                               /* init RTXReadItem */ RTX_REQ_INSERT,pid,key,tableid,len,idx);
  read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
  return idx;
}

int OCC::add_batch_write(int tableid,uint64_t key,int pid,int len) {
  // add a batch read request
  int idx = read_set_.size();
  add_batch_entry<RTXReadItem>(read_batch_helper_,pid,
                               /* init RTXReadItem */ RTX_REQ_READ_LOCK,pid,key,tableid,len,idx);
  read_set_.emplace_back(tableid,key,(MemNode *)NULL,(char *)NULL,0,len,pid);
  return idx;
}


int OCC::send_batch_read(int idx) {
  return send_batch_rpc_op(read_batch_helper_,cor_id_,RTX_READ_RPC_ID);
}

bool OCC::parse_batch_result(int num) {

  char *ptr  = reply_buf_;
  for(uint i = 0;i < num;++i) {
    // parse a reply header
    ReplyHeader *header = (ReplyHeader *)(ptr);
    ptr += sizeof(ReplyHeader);
    for(uint j = 0;j < header->num;++j) {
      OCCResponse *item = (OCCResponse *)ptr;
      read_set_[item->idx].data_ptr = (char *)malloc(read_set_[item->idx].len);
      memcpy(read_set_[item->idx].data_ptr, ptr + sizeof(OCCResponse),read_set_[item->idx].len);
      read_set_[item->idx].seq      = item->seq;
      ptr += (sizeof(OCCResponse) + item->payload);
    }
  }
  return true;
}

void OCC::prepare_write_contents() {

  // Notice that it should contain local records
  // This function has to be called after lock + validation success
  write_batch_helper_.clear_buf(); // only clean buf, not the mac_set

  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    add_batch_entry_wo_mac<RtxWriteItem>(write_batch_helper_,
                                         (*it).pid,
                                         /* init write item */ (*it).pid,(*it).tableid,(*it).key,(*it).len);
    memcpy(write_batch_helper_.req_buf_end_,(*it).data_ptr,(*it).len);
    write_batch_helper_.req_buf_end_ += (*it).len;
  }
}

void OCC::write_back(yield_func_t &yield) {

  // write back local records
  int written_items = 0;
#if 1
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if(it->pid == node_id_) {
      inplace_write_op(it->node,it->data_ptr,it->len);
      written_items += 1;
    }
  }
  if(written_items == write_set_.size()) {// not remote records
    worker_->indirect_yield(yield);
    return;
  }
#endif
  // send the remote records
  send_batch_rpc_op(write_batch_helper_,cor_id_,RTX_COMMIT_RPC_ID,PA);
  assert(write_batch_helper_.mac_set_.size() > 0);
#if PA == 0
  worker_->indirect_yield(yield);
#else
  write_batch_helper_.req_buf_ = rpc_->get_fly_buf(cor_id_); // update the buf to avoid on-flight overwrite
#endif
}

void OCC::write_back_oneshot(yield_func_t &yield) {

  char *cur_ptr = write_batch_helper_.req_buf_;
  START(commit);
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    if((*it).pid != node_id_) {

      CommitItem *item = (CommitItem *)cur_ptr;

      item->tableid = (*it).tableid;
      item->key = (*it).key;
      item->len = (*it).len;

      memcpy(cur_ptr + sizeof(CommitItem),it->data_ptr,it->len);

#if !PA
      rpc_->prepare_multi_req(write_batch_helper_.reply_buf_,1,cor_id_);
#endif
      rpc_->append_pending_req(cur_ptr,RTX_COMMIT_RPC_ID,sizeof(CommitItem) + it->len,cor_id_,RRpc::REQ,(*it).pid);

      cur_ptr += (sizeof(RtxLockItem) + it->len + rpc_->rpc_padding());
    } else {
      inplace_write_op(it->node,it->data_ptr,it->len);
    }
  }
  rpc_->flush_pending();

  worker_->indirect_yield(yield);
  END(commit);
}

bool OCC::release_writes(yield_func_t &yield) {
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

void OCC::log_remote(yield_func_t &yield) {

  if(write_set_.size() > 0 && global_view->rep_factor_ > 0) {

    // re-use write_batch_helper_'s data structure
    BatchOpCtrlBlock cblock(write_batch_helper_.req_buf_,write_batch_helper_.reply_buf_);
    cblock.batch_size_  = write_batch_helper_.batch_size_;
    cblock.req_buf_end_ = write_batch_helper_.req_buf_end_;

#if EM_FASST
    global_view->add_backup(response_node_,cblock.mac_set_);
    ASSERT(cblock.mac_set_.size() == global_view->rep_factor_)
        << "FaSST should uses rep-factor's log entries, current num "
        << cblock.mac_set_.size() << "; rep-factor " << global_view->rep_factor_;
#else
    for(auto it = write_batch_helper_.mac_set_.begin();
        it != write_batch_helper_.mac_set_.end();++it) {
      global_view->add_backup(*it,cblock.mac_set_);
    }
    // add local server
    global_view->add_backup(current_partition,cblock.mac_set_);
#endif

#if CHECKS
    LOG(3) << "log to " << cblock.mac_set_.size() << " macs";
#endif

    START(log);
    logger_->log_remote(cblock,cor_id_);
    worker_->indirect_yield(yield);
    END(log);
#if 1
    cblock.req_buf_ = rpc_->get_fly_buf(cor_id_);
    memcpy(cblock.req_buf_,write_batch_helper_.req_buf_,write_batch_helper_.batch_msg_size());
    cblock.req_buf_end_ = cblock.req_buf_ + write_batch_helper_.batch_msg_size();
    //log ack
    logger_->log_ack(cblock,cor_id_); // need to yield
#endif
  } // end check whether it is necessary to log
}



bool OCC::validate_reads(yield_func_t &yield) {

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
#if !NO_ABORT
      return false;
#endif
    }
  }
  return true;
}

bool OCC::lock_writes(yield_func_t &yield) {

  START(lock);
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
  END(lock);

  // parse the results
  for(uint i = 0;i < write_batch_helper_.mac_set_.size();++i) {
    if(*(get_batch_res<uint8_t>(write_batch_helper_,i)) == LOCK_FAIL_MAGIC) { // lock failed
#if !NO_ABORT
      return false;
#endif
    }
  }
  return true;
}

/* RPC handlers */
void OCC::read_rpc_handler(int id,int cid,char *msg,void *arg) {
  char* reply_msg = rpc_->get_reply_buf();
  char *reply = reply_msg + sizeof(ReplyHeader);
  int num_returned(0);

  RTX_ITER_ITEM(msg,sizeof(RTXReadItem)) {

    RTXReadItem *item = (RTXReadItem *)ttptr;

    if(item->pid != response_node_) {
      continue;
    }

    OCCResponse *reply_item = (OCCResponse *)reply;

    switch(item->type) {
      case RTX_REQ_READ: {
        // fetch the record
        uint64_t seq;
        auto node = local_get_op(item->tableid,item->key,reply + sizeof(OCCResponse),item->len,seq,
                                 db_->_schemas[item->tableid].meta_len);
        reply_item->seq = seq;
        reply_item->idx = item->idx;
        reply_item->payload = item->len;

        reply += (sizeof(OCCResponse) + item->len);
      }
        break;
      case RTX_REQ_READ_LOCK: {
        uint64_t seq;
        MemNode *node = NULL;

        OCCResponse *reply_item = (OCCResponse *)reply;
        if(unlikely((node = local_try_lock_op(item->tableid,item->key,
                                              ENCODE_LOCK_CONTENT(id,worker_id_,cid + 1))) == NULL)) {
          reply_item->seq = 0;
          reply_item->idx = item->idx;
          reply_item->payload = 0;
          reply += sizeof(OCCResponse);
          break;
        } else {
          reply_item->seq = node->seq;
          reply_item->idx = item->idx;
          reply_item->payload = item->len;

          local_get_op(node,reply + sizeof(OCCResponse),seq,item->len);

          reply += (sizeof(OCCResponse) + item->len);
        }
      }
        break;
      default:
        assert(false);
    }
    num_returned += 1;
  } // end for

  ((ReplyHeader *)reply_msg)->num = num_returned;
  assert(num_returned > 0);
  rpc_->send_reply(reply_msg,reply - reply_msg,id,cid);
  // send reply
}

void OCC::lock_rpc_handler(int id,int cid,char *msg,void *arg) {

  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {

    //ASSERT(num < 25) << "[Lock RPC handler] lock " << num << " items.";

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

  //char *log_buf = next_log_entry(&local_log,32);
  //assert(log_buf != NULL);
  //sprintf(log_buf,"reply to  %d c:%d, \n",id,cid);
  *((uint8_t *)reply_msg) = res;
  rpc_->send_reply(reply_msg,sizeof(uint8_t),id,cid);
}

void OCC::release_rpc_handler(int id,int cid,char *msg,void *arg) {

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


void OCC::commit_rpc_handler(int id,int cid,char *msg,void *arg) {

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

void OCC::validate_rpc_handler(int id,int cid,char *msg,void *arg) {

  char* reply_msg = rpc_->get_reply_buf();
  uint8_t res = LOCK_SUCCESS_MAGIC; // success

  RTX_ITER_ITEM(msg,sizeof(RtxLockItem)) {

    //ASSERT(num < 25) << "[Release RPC handler] lock " << num << " items.";

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

void OCC::commit_oneshot_handler(int id,int cid,char *msg,void *arg) {

  CommitItem *item = (CommitItem *)msg;
  inplace_write_op(item->tableid,item->key,msg + sizeof(CommitItem),item->len);
#if !PA
  char *reply = rpc_->get_reply_buf();
  rpc_->send_reply(reply,sizeof(uint8_t),id,cid);
#endif
}

void OCC::backup_get_handler(int id,int cid,char *msg,void *arg) {

  ReadItem *item = (ReadItem *)msg;
  ASSERT(global_view->is_backup(response_node_,item->pid));

  auto store = logger_->cleaner_.get_backed_store(item->pid);

  MemNode *node = store->stores_[item->tableid]->GetWithInsert((uint64_t)(item->key));
  ASSERT(node->value != NULL);

  char *reply_buf = rpc_->get_reply_buf();
  memcpy(reply_buf,(char *)(node->value),
         store->_schemas[item->tableid].vlen);

  rpc_->send_reply(reply_buf,store->_schemas[item->tableid].vlen,id,cid);
}

void OCC::register_default_rpc_handlers() {
  // register rpc handlers
  ROCC_BIND_STUB(rpc_,&OCC::read_rpc_handler,this,RTX_READ_RPC_ID);
  ROCC_BIND_STUB(rpc_,&OCC::lock_rpc_handler,this,RTX_LOCK_RPC_ID);
  ROCC_BIND_STUB(rpc_,&OCC::release_rpc_handler,this,RTX_RELEASE_RPC_ID);
  //ROCC_BIND_STUB(rpc_,&OCC::commit_rpc_handler,this,RTX_COMMIT_RPC_ID);
  ROCC_BIND_STUB(rpc_,&OCC::commit_oneshot_handler,this,RTX_COMMIT_RPC_ID);
  ROCC_BIND_STUB(rpc_,&OCC::validate_rpc_handler,this,RTX_VAL_RPC_ID);

  ROCC_BIND_STUB(rpc_,&OCC::backup_get_handler,this,RTX_BACKUP_GET_ID);
}


}; // namespace rtx

};
