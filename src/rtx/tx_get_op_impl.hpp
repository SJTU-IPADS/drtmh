#include "tx_config.h"

namespace nocc {
namespace rtx  {
#include <utility> // for forward

// implementations of TX's get operators

inline __attribute__((always_inline))
MemNode * TXOpBase::local_get_op(int tableid,uint64_t key,char *val,int len,uint64_t &seq,int meta) {

  MemNode *node = db_->stores_[tableid]->Get(key);
  assert(node != NULL && node->value != NULL);

retry: // retry if there is a concurrent writer
  char *cur_val = (char *)(node->value);
  seq = node->seq;
#if INLINE_OVERWRITE
  memcpy(val,node->padding + meta,len);
#else
  memcpy(val,cur_val + meta,len);
#endif
  asm volatile("" ::: "memory");
  if( unlikely(node->seq != seq) ) {
    goto retry;
  }
  return node;
}

inline __attribute__((always_inline))
MemNode *TXOpBase::local_insert_op(int tableid,uint64_t key,uint64_t &seq) {
  MemNode *node = db_->stores_[tableid]->GetWithInsert(key);
  assert(node != NULL);
  seq = node->seq;
  return node;
}

inline __attribute__((always_inline))
bool TXOpBase::local_try_lock_op(MemNode *node,int lock_content) {
  assert(lock_content != 0); // 0: not locked
  volatile uint64_t *lockptr = &(node->lock);
  if( unlikely( (*lockptr != 0) ||
                !__sync_bool_compare_and_swap(lockptr,0,lock_content)))
    return false;
  return true;
}

inline __attribute__((always_inline))
MemNode *TXOpBase::local_try_lock_op(int tableid,uint64_t key,int lock_content) {

  MemNode *node = db_->stores_[tableid]->Get(key);
  assert(node != NULL && node->value != NULL);
  if(local_try_lock_op(node,lock_content))
     return node;
  return NULL;
}

inline __attribute__((always_inline))
bool TXOpBase::local_try_release_op(MemNode *node,int lock_content) {
  volatile uint64_t *lockptr = &(node->lock);
  return __sync_bool_compare_and_swap(lockptr,lock_content,0);
}

inline __attribute__((always_inline))
bool TXOpBase::local_try_release_op(int tableid,uint64_t key,int lock_content) {
  MemNode *node = db_->stores_[tableid]->GetWithInsert(key);
  return local_try_release_op(node,lock_content);
}

inline __attribute__((always_inline))
bool TXOpBase::local_validate_op(MemNode *node,uint64_t seq) {
  return (seq == node->seq) && (node->lock == 0);
}

inline __attribute__((always_inline))
bool TXOpBase::local_validate_op(int tableid,uint64_t key,uint64_t seq) {
  MemNode *node = db_->stores_[tableid]->Get(key);
  return local_validate_op(node,seq);
}

inline __attribute__((always_inline))
void TXOpBase::inplace_write_op(MemNode *node,char *val,int len) {
#if INLINE_OVERWRITE
  memcpy(node->padding,val,len);
#else
  if(node->value == NULL)
    node->value = (uint64_t *)malloc(len);
  memcpy(node->value,val,len);
#endif

  // release the locks
  node->seq += 2;
  //assert(node->lock != 0);
  node->lock = 0;
}


inline __attribute__((always_inline))
void TXOpBase::inplace_write_op(int tableid,uint64_t key,char *val,int len) {
  MemNode *node = db_->stores_[tableid]->GetWithInsert(key);
  return inplace_write_op(node,val,len);
}


template <typename REQ,typename... _Args>
inline  __attribute__((always_inline))
uint64_t TXOpBase::rpc_op(int cid,int rpc_id,int pid,
                          char *req_buf,char *res_buf,_Args&& ... args) {
  // prepare the arguments
  *((REQ *)req_buf) = REQ(std::forward<_Args>(args)...);

  // send the RPC
  rpc_->prepare_multi_req(res_buf,1,cid);
  rpc_->append_req(req_buf,rpc_id,sizeof(REQ),cid,RRpc::REQ,pid);
}



// } end class
} // namespace rtx
} // namespace nocc
