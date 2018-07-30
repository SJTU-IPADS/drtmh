#include "tx_config.h"
#include "memdb.h"
#include "rdma_hashext.h"
#include "rdma_hash.hpp"

#include "util/util.h"

using namespace nocc;

void MemDB::AddSchema(int tableid,TABLE_CLASS c,  int klen, int vlen, int meta_len,int num,bool need_cache) {

  int total_len = meta_len + vlen;

  // round up to the cache line size
  auto round_size = CACHE_LINE_SZ * 2;
  total_len = nocc::util::Round<int>(total_len,round_size);

  switch(c) {
  case TAB_BTREE:
    stores_[tableid] = new MemstoreBPlusTree();
    break;
  case TAB_BTREE1:
    stores_[tableid] = new MemstoreUint64BPlusTree(klen);
    break;
  case TAB_SBTREE:
    /* This is a secondary index, it does not need to set schema*/
    /* for backward compatability */
    assert(false);
    break;
  case TAB_HASH: {
    //auto tabp = new drtm::memstore::RdmaHashExt(1024 * 1024 * 8,store_buffer_); //FIXME!! hard coded
    auto tabp = new RHash(num, store_buffer_,need_cache);
    stores_[tableid] = tabp;
    // update the store buffer
    if(store_buffer_ != NULL) {
#if 1
      // store_buffer_ += tabp->size * 2;
      //store_size_ += tabp->size * 2;
      store_buffer_ += tabp->size();
      store_size_   += tabp->size();
      uint64_t M = 1024 * 1024;
      ASSERT(store_size_ < M * RDMA_STORE_SIZE) <<
          "store_size: " << get_memory_size_g(store_size_);
#endif
    }
  }
    break;
  default:
    fprintf(stderr,"Unsupported store type! tab %d, type %d\n",tableid,c);
    exit(-1);
  }
  _schemas[tableid].c = c;
  _schemas[tableid].klen = klen;
  _schemas[tableid].vlen = vlen;
  _schemas[tableid].meta_len = meta_len;
  _schemas[tableid].total_len = total_len;
}

void MemDB::EnableRemoteAccess(int tableid,rdmaio::RdmaCtrl *cm) {
  assert(_schemas[tableid].c == TAB_HASH); // now only HashTable support remote accesses
  assert(store_buffer_ != NULL);           // the table shall be allocated on an RDMA region
  //drtm::memstore::RdmaHashExt *tab = (drtm::memstore::RdmaHashExt *)(stores_[tableid]);
  RHash *tab = (RHash *)stores_[tableid];
  tab->enable_remote_accesses(cm);
}

void MemDB::AddSecondIndex(int index_id, TABLE_CLASS c, int klen) {
  _indexs[index_id] = new MemstoreUint64BPlusTree(klen);
}


uint64_t *MemDB::GetIndex(int tableid, uint64_t key) {
  MemNode *mn = _indexs[tableid]->Get(key);
  if(mn == NULL) return NULL;
  return mn->value;
}

uint64_t *MemDB::Get(int tableid,uint64_t key) {
  MemNode *mn = NULL;
  switch(_schemas[tableid].c) {
  case TAB_SBTREE: {
    assert(false);
    mn = _indexs[tableid]->Get(key);
  }
    break;
  default:
    /* otherwise, using store */
    mn = stores_[tableid]->Get(key);
    break;
  }
  if(mn == NULL)
    return NULL;
  return mn->value;
}

void MemDB::PutIndex(int indexid, uint64_t key,uint64_t *value){
  MemNode *mn = _indexs[indexid]->Put(key,value);
  mn->seq = 2;
}

MemNode *MemDB::Put(int tableid, uint64_t key, uint64_t *value,int len) {

  MemNode *mn = NULL;
#if RECORD_STALE
  auto time = std::chrono::system_clock::now();
#endif
  switch(_schemas[tableid].c) {
  case TAB_SBTREE: {
    assert(false);
    mn = _indexs[tableid]->Put(key,value);
    mn->seq = 2;
#if RECORD_STALE
    mn->time = time;
#endif
  }
    break;
  default:
    mn = stores_[tableid]->Put(key,value);
    mn->seq = 2;
    mn->lock = 0;
#if RECORD_STALE
    mn->time = time;
#endif
    break;
  }
#if INLINE_OVERWRITE
  // put the value in the index
  if(len <= INLINE_OVERWRITE_MAX_PAYLOAD) {
    memcpy(mn->padding,(char *)value + _schemas[tableid].meta_len,len);
  }
#endif
  mn->value = value;
  return mn;
}
