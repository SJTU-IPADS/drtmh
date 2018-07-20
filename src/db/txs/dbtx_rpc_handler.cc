#include "tx_config.h"
#include "dbtx.h"
#include "util/mapped_log.h"
#include "util/printer.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
extern size_t current_partition;
extern size_t total_partition;

#define RPC_LOCK_MAGIC_NUM 73

using namespace nocc::util;
using namespace nocc::rtx;

#define TEST_LOG 0
extern __thread MappedLog local_log;

namespace nocc {
namespace oltp {
  extern int rep_factor;
  extern View* my_view; // replication setting of the data
  extern LogHelper *logger;
}
}

using namespace nocc::oltp;


void DBTX::get_naive_rpc_handler(int id,int cid,char *msg,void *arg) {

  RemoteSet::RequestItem *header = (RemoteSet::RequestItem *)msg;

  char *reply_buf = rpc_->get_reply_buf();
  RemoteSet::ReplyItem *r_header = (RemoteSet::ReplyItem *)reply_buf;
  int vlen = txdb_->_schemas[header->tableid].vlen;
  auto node = txdb_->stores_[header->tableid]->GetWithInsert(header->key);

 retry:
  auto seq = node->seq;
  asm volatile("" ::: "memory");
  uint64_t *tmp_val = node->value;
  asm volatile("" ::: "memory");
  if(seq == 1 || node->seq != seq)
    goto retry;

  if(unlikely(tmp_val == NULL)) {
    fprintf(stdout,"tabied %d, key %lu\n",header->tableid,header->key);
    assert(txdb_->Get(header->tableid,header->key) != NULL);
    assert(false);
  }
  else {
    memcpy((char *)reply_buf + sizeof(RemoteSet::ReplyItem),(char *)tmp_val + META_LENGTH, vlen);
  }

  r_header->seq = seq;
  r_header->node = node;
  r_header->idx  = header->idx;

  rpc_->send_reply(reply_buf,vlen + sizeof(RemoteSet::ReplyItem),id,cid);
}

void DBTX::get_rpc_handler(int id,int cid,char *msg,void *arg) {

  RemoteSet::RequestHeader *g_header = (RemoteSet::RequestHeader *)msg;

  /* prepare reply pointer */
  char *reply_msg = rpc_->get_reply_buf();
  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  /* init traverse */
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = g_header->num;
  //assert(num_items > 0 && num_items <= 15);

#if TEST_LOG
  char *log_buf = next_log_entry(&local_log,32);
  assert(log_buf != NULL);
  sprintf(log_buf,"Server read %lu, \n",num_items);
#endif

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
      uint64_t seq(0);

      MemNode *node;
      if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
        // normal fetch
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key.short_key));
#else
        //node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
        node = txdb_->stores_[header->tableid]->Get((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }

      assert(node != NULL);

      retry:
      seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmp_val = node->value;
      asm volatile("" ::: "memory");
      if(seq == 1 || node->seq != seq)
        goto retry;

#if !INLINE_OVERWRITE
      if(unlikely(tmp_val == NULL)) {
        seq = 0;
      }
      else {
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + META_LENGTH, vlen);
      }
#else
      memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem), node->padding,vlen);
#endif

      reply_item->seq = seq;
      ASSERT_PRINT(seq > 0,stdout,"get seq %lu, at table %d, key %lu\n",seq,header->tableid,(uint64_t)(header->key.short_key));
      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;
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
      //      assert(node->seq != 0);
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
      assert(node->value == NULL);

      reply_item->seq  = node->seq;
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
  /* send reply */
  r_header->num_items_ = num_item_fetched;
  r_header->payload_   = reply_msg_t - reply_msg;
  r_header->partition_id_ = current_partition;
  rpc_->send_reply(reply_msg,r_header->payload_,id,cid);
}


void DBTX::get_lock_rpc_handler(int id,int cid,char *msg,void *arg) {

  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  // prepare reply pointer
  char *reply_msg = rpc_->get_reply_buf();

  char *reply_msg_t = reply_msg + sizeof(RemoteSet::ReplyHeader);
  int num_item_fetched(0);

  // init traverse ptr
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);

  int num_items = header->num;
  struct RemoteSet::ReplyHeader *r_header = (struct RemoteSet::ReplyHeader *)reply_msg;
#if 1
  for(int i = 0;i < num_items;++i) {

    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    if(header->pid != current_partition) {
      continue;
    }
    // Fetching objects
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
        //node = txdb_->stores_[header->tableid]->GetWithInsert((uint64_t)(header->key));
        node = txdb_->stores_[header->tableid]->Get((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }
      assert(node != NULL);
      uint64_t counter = 0;
      retry:
      seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmp_val = node->value;
      asm volatile("" ::: "memory");
      if(seq == 1 || node->seq != seq) {
        goto retry;
      }

      if(unlikely(tmp_val == NULL)) {
        //seq = 0;
      }
      else {
        //memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),(char *)tmp_val + META_LENGTH, vlen);
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem), node->padding, vlen);
      }

      reply_item->seq = seq;
      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;

      if(unlikely(seq == 0)){
        //fprintf(stdout,"Tableid %d key %lu\n",);
        assert(false);
      }
    }
      break;
    case REQ_READ_LOCK: {

      RemoteSet::RemoteSetReplyItem *reply_item = (RemoteSet::RemoteSetReplyItem *) reply_msg_t;
      int vlen = txdb_->_schemas[header->tableid].vlen;
      reply_item->payload = vlen;
      uint64_t seq(0);
      MemNode *node;
      assert(txdb_->_schemas[header->tableid].klen < 16);

      if(txdb_->_schemas[header->tableid].klen == sizeof(uint64_t)) {
        // normal fetch
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->Get((uint64_t)(header->key.short_key));
#else
        node = txdb_->stores_[header->tableid]->Get((uint64_t)(header->key));
#endif
      } else {
#if LONG_KEY == 1
        node = txdb_->stores_[header->tableid]->GetWithInsert( (uint64_t )( &(header->key.long_key)));
#else
        assert(false);
#endif
      }

      assert(node != NULL);
      //
#if 1
      //fprintf(stdout,"get lock %lu, tableid %d\n",header->key.short_key,header->tableid);
      volatile uint64_t *lockptr = &(node->lock);
      if( unlikely( (*lockptr != 0) ||
                    !__sync_bool_compare_and_swap(lockptr,0,
                                                  ENCODE_LOCK_CONTENT(id,thread_id,cid + 1))))
        {
          //fprintf(stdout,"key %lu, tableid %d\n",header->key.short_key,header->tableid);
          //assert(false);
          reply_item->seq = 0;
        } else {
        reply_item->seq = node->seq;
        //if(node->seq == 0)
        //          fprintf(stdout,"key %lu, tableid %d node %p\n",header->key.short_key,header->tableid,node);
        assert(node->seq != 1 && node->seq != 0);

        //memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),
        //     (char *)(node->value) + META_LENGTH, vlen);
        memcpy(reply_msg_t + sizeof(RemoteSet::RemoteSetReplyItem),
               node->padding,vlen);
      }
#endif

      reply_item->node = node;
      reply_msg_t += (sizeof(RemoteSet::RemoteSetReplyItem) + vlen);
      reply_item->idx = i;
      num_item_fetched += 1;
    }
      break;
    }
    // end itereating items
  }
  assert(num_item_fetched > 0);
#endif
  /* send reply */
  r_header->num_items_ = num_item_fetched;
  r_header->payload_   = reply_msg_t - reply_msg;
  r_header->partition_id_ = current_partition;
  rpc_->send_reply(reply_msg,r_header->payload_,id,cid);
}


void DBTX::release_rpc_handler(int id,int cid,char *msg,void *arg) {
  /* release rpc handler shall be the same */
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;
  //assert(header->padding == 73);
  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;
  //  fprintf(stdout,"try release  %d\n",num_items);
  for(int i = 0;i < num_items;++i) {
    //    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    //    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);
    if(lheader->pid != current_partition) {
      continue;
    }
#if 1
    /* release the item */
    volatile uint64_t *lockptr = &(lheader->node->lock);
    __sync_bool_compare_and_swap(lockptr,ENCODE_LOCK_CONTENT(id,thread_id,cid + 1),
                                 0);
#endif
  }
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,cid);
}


void DBTX::validate_rpc_handler(int id,int cid,char *msg,void *arg) {
  //assert(false);
  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;

  char *reply_msg = rpc_->get_reply_buf();

  /* initilizae with lock success */
  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 1;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;

  for(int i = 0;i < num_items;++i) {
    RemoteSet::RemoteSetRequestItem *header = (RemoteSet::RemoteSetRequestItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteSetRequestItem);
    if(header->pid != current_partition) {
      continue;
    }
    //    fprintf(stdout,"validate %p for %d\n",header->node,cid);
    /* possibly not happen due to readset != writeset */
    //    assert(header->node->lock == ENCODE_LOCK_CONTENT(id,thread_id,cid + 1));

    if(header->node->seq != header->seq) {
      /* validation failed */
      //      fprintf(stdout,"tableid %d ,seq %lu, needed %lu\n",header->tableid,header->node->seq,
      //	      header->seq);
      ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
      break;
    }
    //fprintf(stdout,"process %d %lu done\n",header->tableid,header->key);
  }
  //  fprintf(stdout,"lock request result %d\n",*((RemoteSet::ReplyHeader *) reply_msg));
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,cid);
}

void DBTX::commit_rpc_handler2(int id,int cid,char *msg,void *arg) {

  int num_items = (*((RTXRequestHeader *) msg)).num;
  uint64_t desired_seq = (*((RTXRequestHeader *) msg)).padding;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  for(uint i = 0;i < num_items;++i) {
    RTXRemoteWriteItem *header = (RTXRemoteWriteItem *)traverse_ptr;
    traverse_ptr += sizeof(RTXRemoteWriteItem);
    if(header->pid != current_partition) {
      traverse_ptr += header->payload;
      continue;
    }
#if 1
    /* now we simply using memcpy */
    char *new_val;
    MemNode *node = txdb_->stores_[header->tableid]->Get((uint64_t)(header->key));

    if(header->payload == 0) {
      /* a delete case */
      new_val = NULL;
    } else {
#if EM_FASST
      // update inplace
      //new_val = (char *)malloc(header->payload + META_LENGTH);
      //memcpy(new_val + META_LENGTH,traverse_ptr,header->payload);
#else
      new_val = (char *)malloc(header->payload + META_LENGTH);
      memcpy(new_val + META_LENGTH,traverse_ptr,header->payload);
#endif
    }
    ASSERT_PRINT(node != NULL,stdout,"error key %lu, tableid %d\n",header->key,header->tableid);
    uint64_t old_seq = node->seq;
    node->seq   = 1;
    asm volatile("" ::: "memory");
#if EM_FASST || INLINE_OVERWRITE
    memcpy(node->padding, traverse_ptr,header->payload);
#else
    //header->node->value = (uint64_t *)new_val;
    node->value = (uint64_t *)new_val;
#endif
    asm volatile("" ::: "memory");
    node->seq = old_seq + 2;
    asm volatile("" ::: "memory");
    /* release the lock */
#if 0
    if(__sync_bool_compare_and_swap( (uint64_t *)(&(header->node->lock)),
                                     ENCODE_LOCK_CONTENT(id,thread_id,cid + 1),
                                     0) != true){
      assert(false);
    }
#else
    node->lock = 0;
#endif

#endif
    traverse_ptr += header->payload;
  }
  //#if COMMIT_NAIVE
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,cid);
  //#endif
}

void DBTX::lock_rpc_handler(int id,int cid,char *msg,void *arg) {

  RemoteSet::RequestHeader *header = (RemoteSet::RequestHeader *)msg;
  char *reply_msg = rpc_->get_reply_buf();

  /* initilizae with lock success */
  ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 1;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  int num_items = header->num;

#if TEST_LOG
  char *log_buf = next_log_entry(&local_log,32);
  assert(log_buf != NULL);
  sprintf(log_buf,"Server lock %lu, \n",num_items);
#endif

#if 1
  //fprintf(stdout,"try lock %d\n",num_items);
  uint64_t max_time(0);
  for(int i = 0;i < num_items;++i) {
    RemoteSet::RemoteLockItem *lheader = (RemoteSet::RemoteLockItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteLockItem);
    if(lheader->pid != current_partition) {
      continue;
    }
#if 1
    //fprintf(stderr,"node %p from %d\n",lheader->node,lheader->pid);
    /* lock the item */
    volatile uint64_t *lockptr = &(lheader->node->lock);
    /* 73 is a magic number to avoid races */
    if( unlikely( (*lockptr != 0) ||
                  !__sync_bool_compare_and_swap(lockptr,0,
                                                ENCODE_LOCK_CONTENT(id,thread_id,cid + 1))))
      {
        /* lock failed */
        ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
        break;
      }
    //    fprintf(stdout,"lock %p for %d\n",lheader->node,cid);
#endif

    // further check sequence numbers
    if(unlikely( lheader->node->seq != lheader->seq)) {
      // validation failed, value has been changed
      ((RemoteSet::ReplyHeader *) reply_msg)->num_items_ = 0;
      break;
    }

    /* end iterating request items */
  }

  /* re-use payload field to set the max time */
  ((RemoteSet::ReplyHeader *)(reply_msg))->payload_ = max_time;
#endif
  //fprintf(stdout,"send back %d\n",sizeof(RemoteSet::ReplyHeader));
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,cid);
}


void DBTX::log_clean_rpc_handler(int id,int cid,char *msg,void *arg) {

  int num_items = (*((RemoteSet::RequestHeader *) msg)).num;

  char *traverse_ptr = msg + sizeof(RemoteSet::RequestHeader);
  assert(num_items > 0);
  for(uint i = 0;i < num_items;++i) {

    RemoteSet::RemoteWriteItem *header = (RemoteSet::RemoteWriteItem *)traverse_ptr;
    traverse_ptr += sizeof(RemoteSet::RemoteWriteItem);

    if(!my_view->response_back(current_partition,header->pid)){
      traverse_ptr += header->payload;
      continue;
    }
    auto store = logger->get_backed_store(header->pid);
    assert(store != NULL);
    ASSERT_PRINT(header->tableid == 1 || header->tableid == 2,stdout,
                 "tabled id %d, num item processed %d %d\n",header->tableid,num_items,i);
    MemNode *node = store->stores_[header->tableid]->Get((uint64_t)(header->key));
    if(node == NULL) {
      fprintf(stderr,"backup key error %lu\n",header->key);
      assert(false);
    }
    char *new_val;

    if(header->payload == 0) {
      /* a delete case */
      new_val = NULL;
    } else {
#if EM_FASST
#else
      new_val = (char *)malloc(header->payload + META_LENGTH);
      memcpy(new_val + META_LENGTH,traverse_ptr,header->payload);
#endif
    }

    uint64_t old_seq = node->seq;
    node->seq   = 1;
    asm volatile("" ::: "memory");
#if EM_FASST || INLINE_OVERWRITE
    memcpy(node->padding, traverse_ptr,header->payload);
#else
    node->value = (uint64_t *)new_val;
#endif
    asm volatile("" ::: "memory");
    node->seq = old_seq + 2;
    asm volatile("" ::: "memory");
    /* release the lock */
    node->lock = 0;
    traverse_ptr += header->payload;
  } // end iterating

  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(RemoteSet::ReplyHeader),id,cid);
}
