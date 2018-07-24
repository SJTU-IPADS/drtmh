/*
 * This file contains a db class built from memstore
 */


#ifndef MEM_DB
#define MEM_DB

#include <stdint.h>

#include "memstore.h"

#include "rdmaio.h"

/* For main table */
#include "memstore_bplustree.h"
/* For string index */
#include "memstore_uint64bplustree.h"
#include "memstore_hash.h"

#define MAX_TABLE_SUPPORTED 32

enum TABLE_CLASS {
  TAB_BTREE,
  TAB_BTREE1,
  // String b+ tree
  TAB_SBTREE,
  TAB_HASH
};

class MemDB {

 public:

  struct TableSchema {

    bool versioned;
    int klen;

    // We didn't support varible length of value
    int vlen;

    // The size of meta data
    int meta_len;

    // Total legnth, = round( meta_len + vlen ) up to cache_line size
    int total_len;

    // Which class of underlying store is used
    TABLE_CLASS c;
  };

  TableSchema _schemas[MAX_TABLE_SUPPORTED]; // table's meta data infor
  Memstore * stores_[MAX_TABLE_SUPPORTED];   // primary tables
  MemstoreUint64BPlusTree *_indexs[MAX_TABLE_SUPPORTED]; // secondary indexes
  char *store_buffer_;  // RDMA buffer used to store the memory store
  

  /*
    Do not give the same store_buffer to different MemDB instances!
  */
  MemDB(char *s_buffer = NULL): store_buffer_(s_buffer) { }

  // expected_num: the number of records in table
  void AddSchema(int tableid, TABLE_CLASS c, int klen,int vlen,int meta_len,int expected_num = 1024,bool need_cache = true);

  /**
     Important!
     If the remote accesses are enabled, then each node shall ensure the order
     of addschema is the same.
     For example,
     if we have 2 tables, A,B.
     Node 0 shall call AddSchema(A),AddSchema(B).
     Node 1 shall call AddSchema(A),AddSchema(B), the same order.

     Further, the store_buffer_ offset shall be the same.
     We do this for convenience.
     Otherwise nodes need sync about the offsets of the RDMA region in the table.
   */
  void EnableRemoteAccess(int tableid,rdmaio::RdmaCtrl *cm);

  void AddSecondIndex(int index_id,TABLE_CLASS c, int klen);
  uint64_t *Get(int tableid,uint64_t key);
  uint64_t *GetIndex(int tableid,uint64_t key);
  MemNode  *Put(int tableid,uint64_t key,uint64_t *value,int len = 0);
  void      PutIndex(int indexid,uint64_t key,uint64_t *value);

  uint64_t store_size_ = 0; // store size alloced on the RDMA area
};

#endif
