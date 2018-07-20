#ifndef NOCC_TX_SI_
#define NOCC_TX_SI_

#include <stdint.h>
#include "all.h"
#include "tx_config.h"
#include "memstore/memdb.h"
#include "tx_handler.h"
#include "db/remote_set.h"
#include "ts_manager.hpp"

#include "core/rworker.h"
#include "core/utils/records_buffer.hpp"

/* using an vector timestamp?*/
#define SI_VEC

struct _SIValHeader {
    uint64_t *oldValue;
    /* If oldValue == NULL, then version = node->seq */
    uint64_t  version;
#if RECORD_STALE
    std_time_t_  time;   // the time that written the value
#endif
};
#define SI_META_LEN  (sizeof(_SIValHeader))

namespace nocc  {
namespace db {

extern TSManager *ts_manager;

int SIGetMetalen();

class DBSI : public TXHandler {
  public:
    /* The global init shall be called before any create of DBRad class */
    static void GlobalInit();
    DBSI(MemDB *tables, int tid,RRpc *rpc,int c_id = 0) ;
    void ThreadLocalInit();

    void _begin(DBLogger *db_logger,TXProfile *p );
    virtual void local_ro_begin();
    bool end(yield_func_t &yield);
    void abort();

    /* local get*/
    uint64_t get(int tableid, uint64_t key, char** val,int len);
    uint64_t get_cached(int tableid,uint64_t key,char **val);

    /* yield is used to cope with very rare read locked objects */
    uint64_t get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield);
    uint64_t get_ro_versioned(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield);

    void write(int tableid,uint64_t key,char *val,int len);
    void write();

    /* Currently only support local insert */
    void insert(int tableid,uint64_t key,char *val,int len);
    void insert_index(int tableid,uint64_t key,char *val);

    void delete_(int tableid,uint64_t key);
    void delete_index(int tableid,uint64_t key);
    void delete_by_node(int tableid, char *node);

    int  add_to_remote_set(int tableid,uint64_t key,int pid);
    int  add_to_remote_set(int tableid,uint64_t *key,int klen,int pid);

    int  remote_read_idx(int tableid,uint64_t *key,int klen,int pid);
    int  remote_insert(int tableid,uint64_t *key, int klen,int pid);
    int  remote_insert_idx(int tableid,uint64_t *key,int klen,int pid);
    void remote_write(int idx,char *val,int len);
    void remote_write(int tableid,uint64_t key,char *val,int len);


    uint64_t get_cached(int r_idx,char **val);
    /* do remote reads can be seen as a combined version of do_remote_reads() + get_remote_results */
    void do_remote_reads(yield_func_t &yield);
    int  do_remote_reads();
    void get_remote_results(int);

    /* RPC handlers */
    void get_rpc_handler(int id,int cid,char *msg,void *arg);
    void get_rpc_handler2(int id,int cid,char *msg,void *arg); // for debug only

    void lock_rpc_handler(int id,int cid,char *msg,void *arg);
    void release_rpc_handler(int id,int cid,char *msg,void *arg);
    void commit_rpc_handler(int id,int cid,char *msg,void *arg);
    void commit_rpc_handler2(int id,int cid,char *msg,void *arg);

    void acquire_ts_handler(int id,int cid,char *msg,void *arg);

    class WriteSet;

    WriteSet *rwset;
    bool localinit;

    bool abort_;
    MemDB *txdb_ ;
    RRpc *rpc_;
    DBLogger *db_logger_;

    /* This shall be 8 byte, or encoding tx id shall report an error
       Though we can use lower bits :)
    */
    uint64_t thread_id;
    std::vector<Qp *> qp_vec_;

    /* buffer used to receive timestamp */
    uint64_t  *ts_buffer_;
    void  get_ts_vec() {
        ThreadLocalInit();
        ts_manager->get_start_ts((char *)ts_buffer_);
    }
    uint64_t get_commit_ts(yield_func_t &yield,uint64_t &encoded_commit_ts);

#if RECORD_STALE
    RecordsBuffer<double> stale_time_buffer;
#endif

  private:
    uint64_t _get_ro_versioned_helper(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield);
    void commit_ts(uint64_t ts,yield_func_t &yield);
};

class SIIterator : public TXIterator {
  public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit SIIterator(DBSI* tx, int tableid,bool sec = false);
    ~SIIterator() {
        delete iter_;
    }

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid();

    // Returns the key at the current position.
    // REQUIRES: Valid()
    uint64_t Key();

    char   * Value();

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

    char   * Node();

  private:
    DBSI* tx_;
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
} // namespace nocc



#endif
