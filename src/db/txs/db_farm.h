#ifndef _TX_FARM_OCC
#define _TX_FARM_OCC

#include <stdint.h>

#include "all.h"

#include "core/rdma_sched.h"
#include "core/rrpc.h"
#include "core/utils/util.h"

#include "tx_handler.h"

#include "db/remote_set.h"
#include "db/db_one_remote.h"

#include "db/db_logger.h"

#include "memstore/memdb.h"

#include "db/db_statistics_helper.h"

namespace nocc {
    namespace db {

    class DBFarm : public TXHandler {

      struct ReqHeader {
        int8_t  num;
        uint8_t cor_id;
      } __attribute__ ((aligned (8)));

    public:
      DBFarm(rdmaio::RdmaCtrl *cm,RDMA_sched *sched,MemDB *tables, int t_id,RRpc *rpc,int c_id = 0); // constructor
      void thread_local_init();                      // local init constructor

      bool lock_remote(yield_func_t &yield);

      bool validate_remote(yield_func_t &yield) { return rrwset_->validate_reads(yield);}

      void release_remote();
      void commit_remote();

      /* remote operators ****************************************************/
      void _begin(DBLogger *db_logger,TXProfile *p); // begin the TX
      void local_ro_begin();                         // begin a ro TX
      bool end(yield_func_t &yield);                 // end the TX
      void abort();                                  // abort the TX

      /* local get*/
      uint64_t get(int tableid, uint64_t key, char** val,int len);
      uint64_t get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield);
      uint64_t get_ro_versioned(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield)
      { assert(false); }

      void write(int tableid,uint64_t key,char *val,int len);
      void write();

      /* Currently only support local insert */
      void insert(int tableid,uint64_t key,char *val,int len);
      void insert_index(int idx_id,uint64_t key,char *val);
      void delete_(int tableid,uint64_t key);
      void delete_by_node(int tableid,char *node) { assert(false);}
      void delete_index(int tableid,uint64_t key) { assert(false);}

      /* remote methods */
      int  add_to_remote_set(int tableid,uint64_t key,int pid);
      int  add_to_remote_set(int tableid,uint64_t key,int pid, yield_func_t &yield);
      int  add_to_remote_set(int tableid,uint64_t *key,int klen,int pid) { assert(false);}

      int  add_to_remote_set_imm(int tableid,uint64_t key,int pid);

      int  remote_read_idx(int tableid,uint64_t *key,int klen,int pid)   { assert(false);}
      void remote_write(int tableid,uint64_t key,char *val,int len) { assert(false);}
      void remote_write(int r_idx,char *val,int len);

      /* below 2 are alike to a distributed version of GetWithInsert */
      int  remote_insert(int tableid,uint64_t *key,int klen,int pid) { assert(false);}
      int  remote_insert_idx(int tableid,uint64_t *key,int klen,int pid) { assert(false);}

      void get_remote_results(int ){
            //    FaRM do nothing since one-sided RDMA reads does not
            // require parse RPC replies.
            return;
      }

      void get_imm_res(int idx,char **ptr,int size);

      // spawn reads to others
      void do_remote_reads(yield_func_t &yield);
      int  do_remote_reads();

      uint64_t get_cached(int idx, char **val);
      uint64_t get_cached(int tableid,uint64_t key,char **val);

      virtual void report();

      class RWSet;  // local  read-write set

    private:
      MemDB *txdb_; friend class DBFarmIterator; // allow iterator to access private *store*
      bool abort_;
      int thread_id;
      RRpc *rpc_handler_;
      DBLogger *db_logger_;
      RDMA_sched *sched_;

      RWSet  *rwset_;
      RRWSet *rrwset_;

      bool localinit;     // whether thread local variable has been initialized
      char *base_ptr_;    // start address of the registered RDMA buffer

      // these structures are used for PRC related stuffs
      char *lock_buf_;       // buffer used to send lock & release message
      char *commit_buf_;     // buffer used to send commit message

      char *lock_buf_end_;   // end ptr of the current lock message
      char *commit_buf_end_; // end ptr of the current commit message
      char *reply_buf_;      // the reply buf of the msg

      std::set<int> server_set_;
      int read_servers_[MAX_SERVER_TO_SENT];
      int read_server_num_;
      int write_servers_[MAX_SERVER_TO_SENT];
      int write_server_num_;

      int write_items_;



      // some less important local variables
      rdmaio::RdmaCtrl *cm_;
#if LOCAL_LOCK_USE_RDMA
      util::fast_random *random_ = NULL;
      Qp* local_qp_;
#endif

      /* RPC handlers ********************************************************/
      void lock_rpc_handler(int id,int cid, char *msg,void *arg);
      void release_rpc_handler(int id,int cid,char *msg,void *arg);
      void commit_rpc_handler(int id,int cid,char *msg,void *arg);

      /* Some statictics  ****************************************************/
      LAT_VARS(lock);
    }; // end class DBFarm



    // DBFarm uses the same itereator implementation of DBTX.

    /* A transaction wrapper of dbiterator which will not prevent phantom */
    class DBFarmIterator : public TXIterator {
    public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit DBFarmIterator(DBFarm* tx, int tableid,bool sec = false);
      ~DBFarmIterator() { delete iter_; }

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
      DBFarm* tx_;
      MemNode* cur_;
      uint64_t *val_;
      uint64_t *prev_link;
      Memstore::Iterator *iter_;

      static inline bool ValidateValue(uint64_t *value) { return value != NULL;}
    }; // end class iterator

    // end namespace db
  };
  // end namespace nocc
};


#endif
