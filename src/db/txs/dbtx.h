#ifndef NOCC_TX_OCC_
#define NOCC_TX_OCC_

#include "tx_handler.h"
#include "db/remote_set.h"
#include "memstore/memdb.h"

#include <map>

#define VALUE_OFFSET 16
#define TIME_OFFSET  0
#define META_LENGTH  16

#define SEQ_OFFSET  8

//#define DEBUG

using namespace nocc::db;

#define MAX_REMOTE_ITEM  4096
namespace nocc {
  namespace db{

class RemoteHelper;

class DBTX : public TXHandler {

 public:

  DBTX(MemDB *tables, int t_id,RRpc *rpc,int c_id = 0);
  DBTX(MemDB *db);

  void ThreadLocalInit();
  void init_temp();
  void reset_temp();

  void _begin(DBLogger *db_logger,TXProfile *p);
  virtual void local_ro_begin();
  /* return true if the transaction successfully commit */
  bool end(yield_func_t &yield);
  bool end_fasst(yield_func_t &yield);

  bool end_ro();
  void abort();

  /* local get*/
  uint64_t get(int tableid, uint64_t key, char** val,int len);
  uint64_t get(char *node,char **val,int len);
  uint64_t get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield);
  uint64_t get_ro_versioned(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield);

  void write(int tableid,uint64_t key,char *val,int len);
  void write();

  /* Currently only support local insert */
  void insert(int tableid,uint64_t key,char *val,int len);
  void insert_index(int idx_id,uint64_t key,char *val);
  void delete_(int tableid,uint64_t key);
  void delete_by_node(int tableid,char *node);
  void delete_index(int tableid,uint64_t key);

  /* remote methods */
  int  add_to_remote_set(int tableid,uint64_t key,int pid);
  int  add_to_remote_set(int tableid,uint64_t *key,int klen,int pid);
  int  remote_read_idx(int tableid,uint64_t *key,int klen,int pid);
  void remote_write(int tableid,uint64_t key,char *val,int len);
  void remote_write(int r_idx,char *val,int len);

  /* below 2 are alike to a distributed version of GetWithInsert */
  int  remote_insert(int tableid,uint64_t *key,int klen,int pid);
  int  remote_insert_idx(int tableid,uint64_t *key,int klen,int pid);


  void do_remote_reads(yield_func_t &yield);
  int  do_remote_reads();
  void get_remote_results(int );

  uint64_t get_cached(int idx, char **val);
  uint64_t get_cached(int tableid,uint64_t key,char **val);

  // report things
  virtual int report_rw_set();
  virtual void report() { if(remoteset != NULL) remoteset->report();}

  /* RPC handlers */
  void get_rpc_handler(int id,int cid,char *msg,void *arg);
  void get_naive_rpc_handler(int id,int cid, char *msg,void *arg);
  void get_lock_rpc_handler(int id,int cid, char *msg,void *arg);

  void lock_rpc_handler(int id,int cid,char *msg,void *arg);
  void release_rpc_handler(int id,int cid,char *msg,void *arg);
  void validate_rpc_handler(int id,int cid,char *msg,void *arg);
  void commit_rpc_handler2(int id,int cid,char *msg,void *arg);
  void ro_val_rpc_handler(int id, int cid,char *msg,void *arg);

  bool debug_validate();
  //  uint64_t HashextTravel(int pid,int tableid,uint64_t key);

  class ReadSet;     /* Used for handling phantom problem */
  class RWSet;
  class DeleteSet;

  ReadSet* readset;
  RWSet *rwset;
  bool localinit;

  bool abort_;
  MemDB *txdb_;

  int thread_id;//TODO!!init
  RRpc *rpc_;
  DBLogger *db_logger_;

#ifdef DEBUG
  TXProfile *profile;
#endif

  static inline bool ValidateValue(uint64_t* value)
  {
    return  ((value != NULL));
  }
};


/* this is used to handle remote requests */
/* TODO!! in this occ style's implementation we do not deal with phantom */
class RemoteHelper {
 public:
  struct RemoteItemSet {
    struct RemoteItem {
      MemNode *node;
      uint64_t seq;
    };
    int num;
    RemoteItem *items ;
    void inline add(MemNode *node,uint64_t seq) {
      assert((num + 1) < MAX_REMOTE_ITEM);
      items[num].node = node;
      items[num++].seq = seq;
    }
    RemoteItemSet() {
      items = new RemoteItem[MAX_REMOTE_ITEM];
      num = 0;
    }
    ~RemoteItemSet() {
      delete items;
    }
  };
  //RemoteItemSet *temp_ptr_;
  MemDB *db_;
  DBTX *temp_tx_;
  int total_servers_;
  int routines_;

  RemoteHelper(MemDB *db,int servers,int coroutines );
  void begin(uint64_t id);
  void add(MemNode *node,uint64_t seq);
  uint16_t validate(uint64_t id);
  void thread_local_init();
};


/* A transaction wrapper of dbiterator */
class DBTXIterator : public TXIterator {
 public:
  // Initialize an iterator over the specified list.
  // The returned iterator is not valid.
  explicit DBTXIterator(DBTX* tx, int tableid,bool sec = false);
  ~DBTXIterator() {
    delete iter_;
  }

  // Returns true iff the iterator is positioned at a valid node.
  bool Valid();

  // Returns the key at the current position.
  // REQUIRES: Valid()
  uint64_t Key();

  char   * Value();
  char   * Node();

  // Advances to the next position.
  // REQUIRES: Valid()
  void Next();

  // Advances to the previous position.
  // REQUIRES: Valid()
  void Prev();

  // Advance to the first entry with a key >= target
  void Seek(uint64_t key);


  // Position at the first entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  void SeekToFirst();

  // Position at the last entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  void SeekToLast();

 private:
  DBTX* tx_;
  //    MemstoreBPlusTree *table_;
  MemNode* cur_;
  uint64_t *val_;
  uint64_t *prev_link;
  Memstore::Iterator *iter_;
  int tableid;
};


/* A transaction wrapper of dbiterator which will not prevent phantom */
class DBTXTempIterator : public TXIterator {
 public:
  // Initialize an iterator over the specified list.
  // The returned iterator is not valid.
  explicit DBTXTempIterator(DBTX* tx, int tableid,bool sec = false);
  ~DBTXTempIterator() {
    delete iter_;
  }

  // Returns true iff the iterator is positioned at a valid node.
  bool Valid();

  // Returns the key at the current position.
  // REQUIRES: Valid()
  uint64_t Key();

  char   * Value();
  char   * Node();

  // Advances to the next position.
  // REQUIRES: Valid()
  void Next();

  // Advances to the previous position.
  // REQUIRES: Valid()
  void Prev();

  // Advance to the first entry with a key >= target
  void Seek(uint64_t key);


  // Position at the first entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  void SeekToFirst();

  // Position at the last entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  void SeekToLast();

 private:
  DBTX* tx_;
  //    MemstoreBPlusTree *table_;
  MemNode* cur_;
  uint64_t *val_;
  uint64_t *prev_link;
  Memstore::Iterator *iter_;

  static inline bool ValidateValue(uint64_t *value) {
    return value != NULL;
  }
};

}
}


#endif
