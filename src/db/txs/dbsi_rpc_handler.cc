#include "dbsi.h"
#include "util/mapped_log.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

extern size_t current_partition;
extern size_t total_partition;

using namespace nocc::db;
using namespace nocc::util;

extern __thread MappedLog local_log;

void
DBSI::get_rpc_handler2(int id,int cid,char *msg,void *arg) {
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;
  assert(header->cor_id == cid);
  /* prepare reply pointer */
  char *reply_msg = rpc_->get_reply_buf();

  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  /* init traverse */
#if 1
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
#else
  /* for test message num */
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader) + sizeof(uint64_t) * total_partition;
#endif
  int num_items = header->num;
  //assert(num_items <= 4);
  assert(num_items > 0);
  assert(num_items == 1);
  struct RemoteSet::ReplyHeader *r_header = (struct RemoteSet::ReplyHeader *)reply_msg;

  for(uint i = 0;i < num_items;++i) {
    //for(uint i = 0;false;) {
    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    if(header->pid != current_partition) {
      continue;
    }
    /* Fetching objects */
    switch(header->type) {
    case REQ_READ: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *) reply_msg_t;
      int vlen = txdb_->_schemas[header->tableid].vlen;
      reply_item->payload = vlen;
      uint64_t seq(0);
      MemNode *node;

      if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
        // normal fetch
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key.short_key));
#else
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }

      //fprintf(stdout,"reply %p to key %lu\n",node,header->key.short_key);
      assert(node != NULL);
      retry:
      asm volatile("" ::: "memory");
      seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmp_val = node->value;
      asm volatile("" ::: "memory");
      if(seq == 1 || node->seq != seq)
        goto retry;

      if(unlikely(tmp_val == NULL)) {
        seq = 0;
        assert(false);
      }
      else {
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + SI_META_LEN, vlen);
      }

      reply_item->seq = seq;
      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;
      assert(seq != 0);
#if RAD_LOG
      char *log_buf = next_log_entry(&local_log,64);
      assert(log_buf != NULL);
      sprintf(log_buf,"try get %d,%d,%d, lock %p\n",id,thread_id,cid,&(node->lock));
#endif
    }
      break;
    case REQ_READ_IDX: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->_indexs[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_INSERT: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_INSERT_IDX: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->_indexs[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_READ_LOCK:
      assert(false);
      break;
    default:
      assert(false);
    }

  }
  //  assert(num_item_fetched > 0 && num_item_fetched <= 256);
  assert(num_item_fetched > 0);
  /* send reply */
  r_header->num_items_ = num_item_fetched;
  r_header->payload_   = reply_msg_t - reply_msg;
  r_header->partition_id_ = current_partition;
  rpc_->send_reply(reply_msg,r_header->payload_,id,thread_id,cid);
}

void DBSI::get_rpc_handler(int id,int cid,char *msg,void *arg) {


  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  /* prepare reply pointer */
  char *reply_msg = rpc_->get_reply_buf();
  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  uint64_t *ts_vec = (uint64_t *)(msg + sizeof(RemoteSet::RequestHeader));
  //TSManager::print_ts((char *)ts_vec,total_partition);

  // init traverse
#ifndef EM_OCC
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader) + ts_manager->ts_size_;
#else
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
#endif

  int num_items = header->num;
  assert(num_items > 0);
  struct RemoteSet::ReplyHeader *r_header = (struct RemoteSet::ReplyHeader *)reply_msg;

  for(int i = 0;i < num_items;++i) {

    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);

    if(header->pid != current_partition) {
      continue;
    }
    /* Fetching objects */
    switch(header->type) {
    case REQ_READ: {

      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *) reply_msg_t;
      int vlen = txdb_->_schemas[header->tableid].vlen;
      reply_item->payload = vlen;
#if 1
      /* occ style's fetching, read committed  */
      {
        MemNode *node;
        if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
          // normal fetch
#if LONG_KEY == 1
          node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key.short_key));
#else
          node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
#endif
        } else {
#if LONG_KEY == 1
          node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
          assert(false);
#endif
        }

        assert(node != NULL);

      retry_1:
        auto seq = node->seq;
        asm volatile("" ::: "memory");
        uint64_t *tmp_val = node->value;
        asm volatile("" ::: "memory");
        if(seq == 1 || node->seq != seq)
          goto retry_1;

        if(unlikely(tmp_val == NULL)) {
          seq = 0;
        }
        else {
          memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + SI_META_LEN, vlen);
        }
        reply_item->seq = seq;
        ASSERT_PRINT(seq > 0,stdout,"get seq %lu, at table %d, key %lu\n",seq,header->tableid,(uint64_t)(header->key.short_key));
        reply_item->node = node;
        reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
        reply_item->idx = i;
        num_item_fetched += 1;
        continue;
      }
#endif
      /* SI style's fetching */
      MemNode *node;
      if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
        // normal fetch
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key.short_key));
#else
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }

      uint64_t ret = 0;

      retry:
      uint64_t seq = node->seq;
      //fprintf(stdout,"reply read %lu, seq %lu\n",(uint64_t)(header->key.short_key),seq);
      if(unlikely(seq == 0)) {
        assert(false);
      }
      /* traverse the read linked list to find the correspond records */
      if(SI_GET_COUNTER(seq) <= ts_vec[SI_GET_SERVER(seq)] ) {
        /* simple case, read the current value */
        asm volatile("" ::: "memory");
        uint64_t *tmpVal = node->value;

        if(likely(tmpVal != NULL)) {
          memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem), (char *)tmpVal + SI_META_LEN,vlen);
          asm volatile("" ::: "memory");
          if(node->seq != seq || seq == 1) {
            goto retry;
          }
        } else {
          /* read a deleted value, currently not supported */
          assert(false);
        }
      } else {
        /* traverse the old reader's list */
        /* this is the simple case, and can always success  */
        char *old_val = (char *)(node->old_value);
        asm volatile("" ::: "memory");
        if(unlikely(node->seq != seq || 1 == seq))
          goto retry;

        _SIValHeader *rh = (_SIValHeader *)old_val;
        while(old_val != NULL && SI_GET_COUNTER(rh->version) > ts_vec[SI_GET_SERVER(rh->version)]) {
          old_val = (char *)(rh->oldValue);
          rh = (_SIValHeader *)old_val;
        }
        if(old_val == NULL) {
          /* cannot find one */
          //fprintf(stdout,"table id %d, get seq %lu, ts vec %lu %lu\n",
          //header->tableid,node->seq, ts_vec[0],ts_vec[1]);
          /* i think it is possible for SI since the ts is staler */
          reply_item->seq = 1;	   /* not valid */
          //assert(false);
          reply_item->payload = 0;
          reply_item->idx = i;
          reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + 0);
          num_item_fetched += 1;
          break;
        } else {
          /* cpy */
          memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)old_val + SI_META_LEN,vlen);
          assert(rh->version != 0);
          seq = rh->version;
          /* in this case, we do not need to check the lock */
        }
      }
#if 1
      reply_item->seq = seq;
      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;
#endif
      assert(seq != 0 && seq != 1);
      /* end read request */
    }
      break;
    case REQ_INSERT: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_READ_IDX: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->_indexs[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
#endif
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    case REQ_INSERT_IDX: {
      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *)reply_msg_t;
#if LONG_KEY == 1
      MemNode *node = txdb_->_indexs[header->tableid]->GetWithInsert((uint64_t)(&(header->key.long_key)));
#else
      MemNode *node = NULL;
      assert(false);
#endif
      reply_item->seq = node->seq;
      reply_item->node = node;
      reply_item->idx  = i;
      reply_item->payload = 0;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem));
      num_item_fetched += 1;
    }
      break;
    default:
      assert(false);
    }
  }
  //fprintf(stdout,"done %d\n",num_item_fetched);
  ASSERT_PRINT(num_item_fetched > 0 && num_item_fetched < 256,
               stderr,"Fetched item %d\n",num_item_fetched);

 REPLY_END:
  /* send reply */
  r_header->num_items_ = num_item_fetched;
  r_header->payload_   = reply_msg_t - reply_msg;
  r_header->partition_id_ = current_partition;
  rpc_->send_reply(reply_msg,r_header->payload_,id,thread_id,cid);
}


void DBSI::lock_rpc_handler(int id,int cid,char *msg,void *arg) {
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  char *reply_msg = rpc_->get_reply_buf();

  /* initilizae with lock success */
  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 1;
#if RAD_LOG
  fprintf(stdout,"try lock  at (%d,%d)\n",id,thread_id);
#endif
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;

  uint64_t max_time(0);
  for(int i = 0;i < num_items;++i) {
    //  RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    //    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);
    if(lheader->pid != current_partition) {
      continue;
    }
#if 1
    /* lock the item */
    volatile uint64_t *lockptr = &(lheader->node->lock);
    /* 73 is a magic number to avoid races */
    if( unlikely( (*lockptr != 0) ||
                  !__sync_bool_compare_and_swap(lockptr,0,
                                                ENCODE_LOCK_CONTENT(id,thread_id,cid + 1))))
      {
        /* lock failed */
        //	fprintf(stdout,"lock failed at %d\n",thread_id);
        ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
        break;
      }
#if 1
    /* further check sequence numbers */
    if(unlikely( lheader->node->seq != lheader->seq)) {
      /* validation failed */
      ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
      break;
    }
#endif
#endif
    //    max_time = MAX(max_time,lheader->node->read_ts);
    /* end iterating request items */
  }
  /* re-use payload field to set the max time */
  assert(num_items > 0);
  //  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = num_items;
  //  ((RemoteSet::ReplyHeader *)(reply_msg))->payload_ = max_time;
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,thread_id,cid);

}


void DBSI::release_rpc_handler(int id,int cid,char *msg,void *arg) {
  /* release rpc handler shall be the same */
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;
#if RAD_LOG
  fprintf(stdout,"try release  %d at (%d,%d)\n",num_items,id,thread_id);
#endif
  for(int i = 0;i < num_items;++i) {
    //    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    //    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);
    if(lheader->pid != current_partition) {
      continue;
    }
#if 1
    //    fprintf(stdout,"try release %p,seq %lu by (%d %d)\n",lheader->node,lheader->node->seq,id,thread_id);
    /* release the item */
    volatile uint64_t *lockptr = &(lheader->node->lock);
    //__sync_bool_compare_and_swap(lockptr,ENCODE_LOCK_CONTENT(id,thread_id,cid),
    //				 0);
    __sync_bool_compare_and_swap(lockptr,ENCODE_LOCK_CONTENT(id,thread_id,cid + 1),0);
#endif
  }
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,thread_id,cid);
}

void DBSI::commit_rpc_handler(int id,int cid,char *msg,void *arg) {
  assert(false);
#if 0
  int32_t total_size   = (*((RemoteSet::CommitHeader *) msg)).total_size;
  uint64_t desired_seq = (*((RemoteSet::CommitHeader *) msg)).commit_seq;

  msg += sizeof(RemoteSet::CommitHeader);
  int processed = 0;
  //  fprintf(stdout,"receive reply size %d\n",total_size);
  assert(total_size > sizeof(RemoteSet::ReplyHeader));
  while(processed < total_size) {

    RemoteSet::ReplyHeader *r_header = (RemoteSet::ReplyHeader *) msg;
    if(r_header->partition_id_ != current_partition) {
      msg += r_header->payload_;
      processed += r_header->payload_;
      continue;
    }

    msg += sizeof(RemoteSet::ReplyHeader);

    for(uint j = 0; j < r_header->num_items_;++j) {
      /* install local writes */
      RemoteSet::RemoteSetReplyItem *header = (RemoteSet::RemoteSetReplyItem *)msg;
      //      memcpy(header->node->value
#if 0
      if(header->seq != header->node->seq) {
        fprintf(stdout,"heade %lu, node %lu\n",header->seq,header->node->seq);
        //	assert(false);
      }
#else
      //      assert(header->seq == header->node->seq);
#endif
      header->node->seq   = 1;
      asm volatile("" ::: "memory");
      /* now we simply using memcpy */
      uint64_t *cur    = header->node->value;
      /* Using value switch */
      uint64_t *oldptr = header->node->old_value;

      if(cur != NULL) {
        _SIValHeader * hptr = (_SIValHeader *)cur;
        hptr->oldValue = oldptr;
        hptr->version  = header->seq;
      } else {

      }
      /* TODO, may set the version */
      char *new_val = (char *)malloc(header->payload + SI_META_LEN);
      memcpy(new_val + SI_META_LEN,msg + sizeof(RemoteSet::RemoteSetReplyItem), header->payload);
      header->node->old_value = cur;
      header->node->value = (uint64_t *)new_val;

      asm volatile("" ::: "memory");
      header->node->seq = desired_seq;
      asm volatile("" ::: "memory");
      /* release the lock */
#if 0
      if(__sync_bool_compare_and_swap( (uint64_t *)(&(header->node->lock)),
                                       _QP_ENCODE_ID(id,this->thread_id + 1),
                                       0) != true){
        fprintf(stdout,"locked by %d idx %d\n",_QP_DECODE_MAC(header->node->lock),_QP_DECODE_INDEX(header->node->lock));
        assert(false);
      }
#else
      header->node->lock = 0;
#endif
      //header->node->lock = 0; // optimized way
      msg += (header->payload + sizeof(RemoteSet::RemoteSetReplyItem));
    }
    break;
  }
  assert(processed < total_size);
  /* end proessing commit message */
#endif
}


void DBSI::commit_rpc_handler2(int id,int cid,char *msg,void *arg) {

  int num_items = (*((RemoteSet::RequestHeader *) msg)).num;
  uint64_t desired_seq = (*((RemoteSet::RequestHeader *) msg)).padding;
  //assert(desired_seq == 12);
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);

  for(uint i = 0;i < num_items;++i) {
    RemoteSet::RemoteWriteItem *header = (RemoteSet::RemoteWriteItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteWriteItem);
    if(header->pid != current_partition ) {
      traverse_ptr += header->payload;
      continue;
    }
#if 1
    char *new_val;
    if(header->payload == 0)
      new_val = NULL;
    else {
      new_val = (char *)malloc(header->payload + SI_META_LEN);
      memcpy(new_val + SI_META_LEN,traverse_ptr, header->payload);
    }

    uint64_t old_seq = header->node->seq;
    header->node->seq   = 1;
    asm volatile("" ::: "memory");
    /* now we simply using memcpy */
    uint64_t *cur    = header->node->value;
    /* Using value switch */
    uint64_t *oldptr = header->node->old_value;

    if(cur != NULL) {
      _SIValHeader * hptr = (_SIValHeader *)cur;
      hptr->oldValue = oldptr;
      hptr->version = old_seq;
    } else {

    }
    header->node->old_value = cur;
    header->node->value = (uint64_t *)new_val;
    //ASSERT_PRINT(header->node->seq < desired_seq,stderr,
    //           "header %lu, desired %lu",header->node->seq,desired_seq);
    asm volatile("" ::: "memory");
#ifndef EM_OCC
    header->node->seq = desired_seq;
#else
    header->node->seq = header->node->seq + 1;
#endif
    asm volatile("" ::: "memory");
    /* release the lock */
    //    fprintf(stdout,"try release %p at %d\n",header->node,thread_id);
#if 0
    //    fprintf(stdout,"try commit release %p , seq %lu by (%d %d)\n",header->node,header->node->seq,
    //	    id,thread_id);
    if(__sync_bool_compare_and_swap( (uint64_t *)(&(header->node->lock)),
                                     _QP_ENCODE_ID(id,this->thread_id + 1),
                                     0) != true) {
      fprintf(stdout,"%p locked by %d idx %d at %d\n",header->node,
              _QP_DECODE_MAC(header->node->lock),_QP_DECODE_INDEX(header->node->lock),thread_id);
      assert(false);
    }
#else
    //    assert(false);
    header->node->lock = 0;
#endif

#endif
    traverse_ptr += header->payload;
  }
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,thread_id,cid);
}


void DBSI::acquire_ts_handler(int id,int cid,char *msg,void *arg) {

  assert(current_partition == ts_manager->master_id_);

  char *reply_msg = rpc_->get_reply_buf();
  uint64_t res = ts_manager->get_commit_ts();
  *((uint64_t *)reply_msg) = res;

  rpc_->send_reply(reply_msg,sizeof(uint64_t),id,thread_id,cid);
}
