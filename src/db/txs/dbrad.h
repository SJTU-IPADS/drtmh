#ifndef _TX_RAD_
#define _TX_RAD_

#include <stdint.h>

#include "all.h"
#include "global_config.h"
#include "memstore/memdb.h"
#include "tx_handler.h"
#include "db/remote_set.h"
#include "db/db_logger.h"

#define RAD_META_LEN  (sizeof(_RadValHeader))
struct _RadValHeader {
    uint64_t *oldValue;
    /* If oldValue == NULL, then version = node->seq */
    uint64_t  version;
};


namespace nocc  {
    namespace db {

        int radGetMetalen();

        class DBRad : public TXHandler {
        public:
            /* The global init shall be called before any create of DBRad class */
            static void GlobalInit();
            DBRad(MemDB *tables, int tid,RRpc *rpc,int c_id = 0) ;
            void ThreadLocalInit();
  
            void _begin(DBLogger *db_logger, TXProfile *p );
            virtual void local_ro_begin();
            bool end(yield_func_t &yield);
            bool end_fasst(yield_func_t &yield);
            void abort();

            /* local get*/
            uint64_t get(int tableid, uint64_t key, char** val,int len);
            uint64_t get_cached(int tableid,uint64_t key,char **val);
            uint64_t get_cached(int idx, char **val);
  
            /* yield is used to cope with very rare read locked objects */
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
            bool check_in_rwset(int tableid,uint64_t key) ;

            /* remote access methods */
            int  add_to_remote_set(int tableid,uint64_t key,int pid);
            int  add_to_remote_set(int tableid,uint64_t *key,int klen,int pid);
            int  remote_read_idx(int tableid,uint64_t *key,int klen,int pid);
    
            /* below 2 are alike to a distributed version of GetWithInsert */
            int  remote_insert(int tableid,uint64_t *key,int klen,int pid);
            int  remote_insert_idx(int tableid,uint64_t *key,int klen,int pid);
    
            void remote_write(int tableid,uint64_t key,char *val,int len);
            void remote_write(int r_idx,char *val,int len );
  
            /* do remote reads can be seen as a combined version of do_remote_reads() + get_remote_results */
            void do_remote_reads(yield_func_t &yield);
            int  do_remote_reads();
            void get_remote_results(int);

            void reset();

            /* RPC handlers */
            void get_rpc_handler(int id,int cid,char *msg,void *arg);
            void lock_rpc_handler(int id,int cid,char *msg,void *arg);
            void release_rpc_handler(int id,int cid,char *msg,void *arg);
            void commit_rpc_handler(int id, int cid,char *msg,void *arg);
            void commit_rpc_handler2(int id,int cid,char *msg,void *arg);

            void fast_get_rpc_handler(int id,int cid,char *msg,void *arg);
            void fast_validate_rpc_handler(int id,int cid,char *msg,void *arg);

            class WriteSet;

            //    static __thread WriteSet *rwset;
            //    static __thread bool localinit;
            //    static __thread RemoteSet *remoteset;
            WriteSet *rwset;
            bool   localinit;
  
            bool abort_;
            MemDB *txdb_ ;
            char padding[64];
            RRpc *rpc_;
            DBLogger *db_logger_;

            uint64_t thread_id;
            uint64_t get_timestamp();

            // Below is how timestamp is managed
        private:
            // last committed timestamp of write transaction, for check
            //    uint64_t counter;
            uint64_t _getTxId();
            uint64_t _get_ro_versioned_helper(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield); // use split read/write lock
            uint64_t _get_ro_naive(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield);     // contend lock with writer
            uint64_t _get_helper(MemNode *node, char *val,uint64_t version,int vlen);

            //int last_rpc_mark_[16];

            //char *last_lock_ptr_[16];
            //uint64_t last_msg_counts_[16];
            //std::map<uint64_t,bool> lock_check_status[16];
        };


        // TX wrapper for the iterator
        class RadIterator : public TXIterator {
        public:
            // Initialize an iterator over the specified list.
            // The returned iterator is not valid.
            explicit RadIterator(DBRad* tx, int tableid,bool sec = false);
            ~RadIterator() {
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
            DBRad* tx_;
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
