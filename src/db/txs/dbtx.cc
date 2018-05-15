#include "dbtx.h"
#include "all.h"
#include "db/config.h"
#include "tx_config.h"
#include "global_config.h"

#include "app/config.h"
#include "framework/bench_worker.hpp"

#include "util/printer.h"

#include <unistd.h>
#include <algorithm>

/* for std::bind */
#include <functional>

#define MAXSIZE 1024

// 1: use copy to install the new value
// 0: replace the old value with the new value pointer
#define COPY 0

using namespace nocc::oltp;
using namespace nocc::util;

extern size_t nthreads;
extern size_t current_partition;

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

/* Read-write sets */
/*******************/

__thread RemoteHelper *remote_helper;
namespace nocc{

  extern __thread BenchWorker* worker;
  namespace db{
    class DBTX::ReadSet  {

    public:
      int elems;
      ReadSet(int tid);
      ~ReadSet();
      inline void Reset();
      inline uint64_t Add(uint64_t *ptr);
      inline void AddNext(uint64_t *ptr, uint64_t value);
      inline bool Validate();

    private:
      struct RSSeqPair {
        uint64_t seq; //seq got when read the value
        uint64_t *seqptr; //pointer to the global memory location
      };

      //This is used to check the insertion problem in range query
      //FIXME: Still have the ABA problem
      struct RSSuccPair {
        uint64_t next; //next addr
        uint64_t *nextptr; //pointer to next[0]
      };

      int max_length;
      RSSeqPair *seqs;

      int rangeElems;
      RSSuccPair *nexts;

      void Resize();
      int tid;
    };

    DBTX::ReadSet::ReadSet(int t):
      tid(t)
    {
      max_length = MAXSIZE;
      elems = 0;
      seqs = new RSSeqPair[max_length];

      rangeElems = 0;
      nexts = new RSSuccPair[max_length];

      //pretouch to avoid page fault in the rtm
      for(int i = 0; i < max_length; i++) {
        seqs[i].seq = 0;
        seqs[i].seqptr = NULL;
        nexts[i].next = 0;
        nexts[i].nextptr = NULL;
      }
    }

    DBTX::ReadSet::~ReadSet()
    {
      delete[] seqs;
      delete[] nexts;
    }

    inline void DBTX::ReadSet::Reset()
    {
      elems = 0;
      rangeElems = 0;
    }

    void DBTX::ReadSet::Resize()
    {
      //    printf("Read Set Resize %d  by %d\n",elems,tid);
      max_length = max_length * 2;

      RSSeqPair *ns = new RSSeqPair[max_length];
      for(int i = 0; i < elems; i++) {
        ns[i] = seqs[i];
      }
      delete[] seqs;
      seqs = ns;


      RSSuccPair* nts = new RSSuccPair[max_length];
      for(int i = 0; i < rangeElems; i++) {
        nts[i] = nexts[i];
      }
      delete[] nexts;
      nexts = nts;

    }


    inline void DBTX::ReadSet::AddNext(uint64_t *ptr, uint64_t value)
    {
      //if (max_length < rangeElems) printf("ELEMS %d MAX %d\n", rangeElems, max_length);
      assert(rangeElems <= max_length);

      if(rangeElems == max_length) {
        Resize();
      }

      int cur = rangeElems;
      rangeElems++;

      nexts[cur].next = value;
      nexts[cur].nextptr = ptr;
    }


    inline uint64_t DBTX::ReadSet::Add(uint64_t *ptr)
    {
      if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
      assert(elems <= max_length);

      if(elems == max_length) {
        Resize();
        //assert(false);
      }

      int cur = elems;
      elems++;

      //    fprintf(stdout,"add seq %llu\n",*ptr);
      // since we are update steps 2 for optimistic replication
      //  seqs[cur].seq = (*ptr + ((*ptr) % 2 ));
      seqs[cur].seq = *ptr;
      seqs[cur].seqptr = ptr;
      return seqs[cur].seq;
    }

    inline bool DBTX::ReadSet::Validate()
    {
      //This function should be protected by rtm or mutex
      //Check if any tuple read has been modified
      for(int i = 0; i < elems; i++) {
        assert(seqs[i].seqptr != NULL);
        if(seqs[i].seq != *seqs[i].seqptr) {
          //      fprintf(stdout,"Record seq %lu, now %lu\n",seqs[i].seq, *seqs[i].seqptr);
          return false;
        }
      }

      //Check if any tuple has been inserted in the range
      for(int i = 0; i < rangeElems; i++) {
        assert(nexts[i].nextptr != NULL);
        if(nexts[i].next != *nexts[i].nextptr) {
          //      fprintf(stdout,"Record seq %lu, now %lu\n",nexts[i].next, *nexts[i].nextptr);
          return false;
        }
      }
      return true;
    }

    /* End readset's definiation */

    class DBTX::DeleteSet {
    public:
      struct DItem {
        int tableid;
        uint64_t key;
        MemNode *node;
      };

      int elems;
      int max_length;
      DItem *kvs;

      DeleteSet();
      inline void Reset();
      inline void Add(DItem &item);
      inline void Execute();
      inline bool Prepare();
    };

    DBTX::DeleteSet::DeleteSet() {
      max_length = MAXSIZE;
      elems = 0;
      kvs = new DItem[max_length];
    }

    inline void DBTX::DeleteSet::Reset() {
      elems = 0;
    }

    inline void DBTX::DeleteSet::Add(DBTX::DeleteSet::DItem &item) {
      kvs[elems].key = item.key;
      kvs[elems].tableid = item.tableid;
      kvs[elems].node = item.node;
    }

    inline bool DBTX::DeleteSet::Prepare() {
      return true;
    }

    inline void DBTX::DeleteSet::Execute() {
      /* Fix me, current we does not consider concurrency in execute function */
      for(uint i = 0;i < elems;++i) {
        if(kvs[i].node->value != NULL) {
          kvs[i].node->value = NULL;
          //      kvs[i].node->seq = 0;
        }
        else
          assert(false);
      }
    }

    /* End delete set's implementation */


    class DBTX::RWSet  {
    public:
      struct RWSetItem {
        int32_t tableid;
        uint64_t key;
        int len;
        MemNode  *node;
        uint64_t *addr;  /* Pointer to the new value buffer */
        uint64_t seq;
        bool ro;
      };

      DBTX *dbtx_;
      int max_length;
      RWSetItem *kvs;

      int elems;
      int current;

      RWSet();
      ~RWSet();

      inline void Reset();
      inline void Resize();
      inline void SetDBTX(DBTX *dbtx);
      inline void Add(RWSetItem &item);

      // commit helper functions
      inline bool LockAllSet();
      inline void ReleaseAllSet();
      inline bool CheckLocalSet();

      inline int  CommitLocalWrite();

      inline void GC();

      bool inline IsLocked(uint64_t *ptr) {
        return (*ptr) != 0;
      }
    };

    DBTX::RWSet::RWSet()
    {
      max_length = MAXSIZE;
      elems = 0;
      kvs = new RWSetItem[max_length];
      dbtx_ = NULL;

      for (int i = 0; i < max_length; i++) {
        kvs[i].tableid = -1;
        kvs[i].key = 0;
        kvs[i].seq = 0;
        kvs[i].addr = NULL;
        kvs[i].len = 0;
        kvs[i].ro = false;
        kvs[i].node = NULL;
      }
    }


    DBTX::RWSet::~RWSet()
    {  delete [] kvs;
    }

    inline void DBTX::RWSet::GC() {
      for(uint i = 0;i < elems;++i) {
        if(kvs[i].addr != NULL) free(kvs[i].addr);
      }
    }

    inline void DBTX::RWSet::Add(RWSetItem &item) {

      //  assert(item.tableid != 0);
      if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
      //assert(elems <= max_length);
      if (elems == max_length) {
        Resize();
        //assert(false);
      }

      int cur = elems;
      elems++;

      //  kvs[cur].pid = item.pid;
      kvs[cur].tableid = item.tableid;
      kvs[cur].key = item.key;
      kvs[cur].len = item.len;
      kvs[cur].ro = item.ro;

      kvs[cur].seq = item.seq;
      //  kvs[cur].loc = item.loc;
      kvs[cur].addr = item.addr;
      //  kvs[cur].off = item.off;
      kvs[cur].node = item.node;
    }

    inline void DBTX::RWSet::SetDBTX(DBTX *dbtx)
    {
      dbtx_ = dbtx;
    }

    inline void DBTX::RWSet::Reset()
    {
      elems = 0;
      current = -1;
    }

    inline void DBTX::RWSet::Resize()
    {
      max_length = max_length * 2;

      RWSetItem *nkvs = new RWSetItem[max_length];
      for (int i = 0; i < elems; i++) {
        nkvs[i] = kvs[i];
      }

      delete []kvs;
      kvs = nkvs;
    }

    inline bool
    DBTX::RWSet::LockAllSet() {

      for(int i = 0;i < elems;++i) {
        if(kvs[i].ro) {
          continue;
        }

        //    uint64_t *lockptr = (uint64_t *)(kvs[i].loc);
        volatile uint64_t *lockptr = &(kvs[i].node->lock);

        //    fprintf(stdout,"lock %d  %lu\n",kvs[i].tableid,kvs[i].key);
        if( unlikely( (*lockptr != 0) ||
                      !__sync_bool_compare_and_swap(lockptr,0,
                                                    ENCODE_LOCK_CONTENT(current_partition,dbtx_->thread_id,
                                                                        dbtx_->cor_id_ + 1)))) {
          //      fprintf(stdout,"prev value %lu at %lu tab %d current %d\n",*lockptr,kvs[i].off,kvs[i].tableid,current_partition);
          return false;
        }
        current = i;
        if(kvs[i].node->seq != kvs[i].seq) {
          return false;
        }
      }
      return true;
    }

    inline void
    DBTX::RWSet::ReleaseAllSet() {

      for(int i = 0;i <= current;++i) {
        if(kvs[i].ro)
          continue;
        volatile uint64_t *lockptr = &(kvs[i].node->lock);
        //    *lockptr = 0;
        __sync_bool_compare_and_swap( lockptr,
                                      ENCODE_LOCK_CONTENT(current_partition,dbtx_->thread_id,
                                                          dbtx_->cor_id_ + 1),0);
      }
    }

    inline bool DBTX::RWSet::CheckLocalSet() {

      for(uint i = 0;i < elems;++i) {

        uint64_t seq = kvs[i].node->seq;

        if(seq != kvs[i].seq) {
          //      fprintf(stdout,"abort by table %d %lu=>%lu\n",kvs[i].tableid,kvs[i].seq,seq);
          //assert(false);
          return false;
        }
      }
      return true;
    }

    inline int
    DBTX::RWSet::CommitLocalWrite() {

      int counter = 0;
      for(uint i = 0;i < elems;++i) {

        if(kvs[i].ro)
          continue;
        /* local writes */
        kvs[i].node->seq = 1;

        asm volatile("" ::: "memory");
#if COPY == 1
        memcpy( (char *)(kvs[i].node->value) + META_LENGTH, (char *)(kvs[i].addr) + META_LENGTH,kvs[i].len);
#else
        kvs[i].node->value = kvs[i].addr;
#endif
        asm volatile("" ::: "memory");
        // update the sequence
        kvs[i].node->seq = kvs[i].seq + 2;

        /* By the way, release the lock */
        asm volatile("" ::: "memory");
        kvs[i].node->lock = 0;
      }
      return counter;
    }


    /* Main transaction layer definations  */
    void DBTX::ThreadLocalInit()
    {
      if ( false == localinit) {
        readset = new ReadSet(thread_id);
        rwset = new RWSet();
        remoteset = new RemoteSet(rpc_,cor_id_,thread_id);
        localinit = true;
      }
    }

    void DBTX::init_temp() {
      readset = new ReadSet(thread_id);
      rwset = new RWSet();
    }

    DBTX::DBTX(MemDB *db) : txdb_(db),localinit(false) ,TXHandler(73) { }

    DBTX::DBTX(MemDB* tables,int t_id,RRpc *rpc,int c_id)
      :txdb_(tables),
       thread_id(t_id),
       rpc_(rpc),
       localinit(false),
       TXHandler(c_id)
    {
      /* register RPC handler */
      using namespace  std::placeholders;
#ifdef EM_FASST
      rpc_->register_callback(std::bind(&DBTX::get_lock_rpc_handler,this,_1,_2,_3,_4),RPC_READ,true);
#else
#if NAIVE == 4
      rpc_->register_callback(std::bind(&DBTX::get_rpc_handler,this,_1,_2,_3,_4),RPC_READ,true);
#else
      rpc_->register_callback(std::bind(&DBTX::get_naive_rpc_handler,this,_1,_2,_3,_4),RPC_READ,true);
#endif // register RPC for normal's case
#endif // register RPC for fasst's case
      rpc_->register_callback(std::bind(&DBTX::lock_rpc_handler,this,_1,_2,_3,_4),RPC_LOCK,true);
      rpc_->register_callback(std::bind(&DBTX::release_rpc_handler,this,_1,_2,_3,_4),RPC_RELEASE,true);
      rpc_->register_callback(std::bind(&DBTX::validate_rpc_handler,this,_1,_2,_3,_4),RPC_VALIDATE,true);
      rpc_->register_callback(std::bind(&DBTX::commit_rpc_handler2,this,_1,_2,_3,_4),RPC_COMMIT,true);

      rpc_->register_callback(std::bind(&DBTX::ro_val_rpc_handler,this,_1,_2,_3,_4),RPC_R_VALIDATE,true);
      nreadro_locked = 0;
    }

    uint64_t DBTX::get_cached(int tableid, uint64_t key, char **val) {
      for(uint i = 0;i < remoteset->elems_;++i) {
        if(remoteset->kvs_[i].tableid == tableid && remoteset->kvs_[i].key == key) {
          //      fprintf(stdout,"return key %lu\n",key);
          *val = (char *)(remoteset->kvs_[i].val);
          return remoteset->kvs_[i].seq;
        }
      }
      return 0;
    }

    uint64_t DBTX::get_cached(int idx,char **val) {
      *val = (char *)(remoteset->kvs_[idx].val);
      return remoteset->kvs_[idx].seq;
    }



    uint64_t DBTX::get_ro_versioned(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield) {
      /* this shall be only called for remote operations */
      /* since this is occ, the version is not useful */
#if 0
      MemNode *node = NULL;
      int len = txdb_->_schemas[tableid].vlen;
      //  fprintf(stdout,"start getwith %p\n",txdb_);
      node = txdb_->stores_[tableid]->GetWithInsert(key);
      assert(node != NULL);
    retry:
      uint64_t seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmpVal = node->value;

      if(likely(tmpVal != NULL)) {
        if(unlikely(seq == 1))
          goto retry;
        asm volatile("" ::: "memory");
        memcpy(val,(char *)tmpVal + META_LENGTH,len );
        asm volatile("" ::: "memory");
        if( unlikely(node->seq != seq) ) {
          goto retry;
        }

      } else {
        //    fprintf(stdout,"key %lu, tableid %d\n",key,tableid);
        //    assert(false);
        //    seq = 0;
        return 1;
      }
      remote_helper_->add(node,seq);
#endif
      return remote_helper->temp_tx_->get_ro(tableid,key,val,yield);
    }

    uint64_t DBTX::get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield) {

      MemNode *node = NULL;
      int len = txdb_->_schemas[tableid].vlen;
      node = txdb_->stores_[tableid]->GetWithInsert(key);
    retry:
      uint64_t seq = node->seq;
      asm volatile("" ::: "memory");
      uint64_t *tmpVal = node->value;

      if(likely(tmpVal != NULL)) {
        if(unlikely(seq == 1))
          goto retry;
        asm volatile("" ::: "memory");
        memcpy(val,(char *)tmpVal + META_LENGTH,len );
        asm volatile("" ::: "memory");
        if( unlikely(node->seq != seq) ) {
          goto retry;
        }

      } else {
        //    fprintf(stdout,"key %lu, tableid %d\n",key,tableid);
        //    assert(false);
        return 1;
      }
#ifdef OCC_RETRY
      //#if 1
      RWSet::RWSetItem item;

      item.tableid = tableid;
      item.key = key;
      item.seq = seq;
      item.len = len;
      item.ro  = true;
      item.node = (MemNode *)node;

      rwset->Add(item);
#endif
      return seq;
    }

    uint64_t DBTX::get(int tableid, uint64_t key, char **val,int len) {

#if 0
      for(uint i = 0;i < rwset->elems;++i) {
        if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
          *val = (char *)rwset->kvs[i].addr;
          return rwset->kvs[i].seq;
        }
      }
#endif
      char *_addr = (char *)malloc(len + META_LENGTH);
      MemNode *node = NULL;
      node = txdb_->stores_[tableid]->GetWithInsert(key);

    retry:
      uint64_t seq = node->seq;

      asm volatile("" ::: "memory");
      if(unlikely(seq == 1)) goto retry;
      uint64_t *tmpVal = node->value;

      if(unlikely(tmpVal == NULL)) {
        return 1;
      }
      //  asm volatile("" ::: "memory");
      memcpy(_addr + META_LENGTH,(char *)tmpVal + META_LENGTH,len);
      asm volatile("" ::: "memory");
      if( unlikely(node->seq != seq) ) {
        goto retry;
      }

#if 1
      RWSet::RWSetItem item;

      item.tableid = tableid;
      item.key = key;
      item.seq = seq;
      item.len = len;
      item.ro  = true;
      item.addr = (uint64_t *)_addr;
      //item.loc  = tmpVal;
      item.node = (MemNode *)node;

      rwset->Add(item);
#endif
      *val = ((char *)_addr + META_LENGTH);
      return seq;
    }


    uint64_t DBTX::get(char *n, char **val,int len) {

      char *_addr = (char *)malloc(len + META_LENGTH);
      MemNode *node = (MemNode *)n;

    retry:
      uint64_t seq = node->seq;
      return seq;
#if 1
      asm volatile("" ::: "memory");
      if(unlikely(seq == 1)) goto retry;
      uint64_t *tmpVal = node->value;

      if(unlikely(tmpVal == NULL)) {
        assert(false);
        return 0;
      }

      //  asm volatile("" ::: "memory");
      memcpy(_addr + META_LENGTH,(char *)tmpVal + META_LENGTH,len);
      asm volatile("" ::: "memory");
      if( unlikely(node->seq != seq) ) {
        goto retry;
      }

#if 1
      RWSet::RWSetItem item;

      item.seq = seq;
      item.len = len;
      item.ro  = true;
      item.addr = (uint64_t *)_addr;
      item.node = (MemNode *)node;

      rwset->Add(item);
#endif
      *val = ((char *)_addr + META_LENGTH);
#endif
      return seq;
    }

#if TX_USE_LOG
    inline void prepare_log(int cor_id, DBLogger* db_logger, const DBTX::RWSet::RWSetItem& item){
      char* val = db_logger->get_log_entry(cor_id, item.tableid, item.key, item.len);
      // printf("%p %p %d %lu\n", val, (char*)item.addr + META_LENGTH, item.len, item.key);
      memcpy(val, (char*)item.addr + META_LENGTH, item.len);
      db_logger->close_entry(cor_id, item.seq + 2);
    }
#endif

    void DBTX::insert_index(int tableid, uint64_t key,char *val) {

      MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
      RWSet::RWSetItem item;
      if(tableid == 4) assert(node->value == NULL);

      item.tableid = tableid;
      item.key     = key;
      item.node    = node;
      item.addr    = (uint64_t *)val;
      item.seq     = node->seq;
      item.ro      = false;
      rwset->Add(item);
    }

    void DBTX::insert(int tableid, uint64_t key, char *val, int len) {

      int vlen = META_LENGTH + len;

      RWSet::RWSetItem item;
      item.addr = (uint64_t *) (new char[vlen]);
      memset(item.addr,0,META_LENGTH);
      memcpy( (char *)item.addr + META_LENGTH, val,len);

    retry:
#if COPY == 1
      MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key,(char *)(item.addr));
#else
      MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
#endif

      item.tableid = tableid;
      item.key = key;
      item.seq = node->seq;
      item.len = len;
      item.ro = false;
      item.node = node;

      rwset->Add(item);
#if TX_USE_LOG
      if(db_logger_){
        prepare_log(cor_id_, db_logger_, item);
      }
#endif
    }

    void DBTX::delete_index(int tableid,uint64_t key) {
#if 0
      DeleteSet::DItem item;
      item.tableid = tableid;
      item.node = txdb_->_indexs[tableid]->GetWithInsert(key);
      if(unlikely(item.node->value == NULL)) return;
      delset->Add(item);
#endif
      MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
      if(unlikely(node->value == NULL)) {
        //fprintf(stdout,"index %d null, check symbol %s using %p\n",
        //	      tableid,(char *)( &(((uint64_t *)key)[2])),node);
        //      assert(false);
        // possible found by the workload
        return;
      }
      RWSet::RWSetItem item;
      item.tableid = tableid;
      item.key     = key;
      item.node    = node;
      item.addr    = NULL;
      item.seq     = node->seq;
      item.ro      = false;
      rwset->Add(item);
    }

    void DBTX::delete_by_node(int tableid,char *node) {
#if 0
      DeleteSet::DItem item;
      item.tableid = tableid;
      item.node = (MemNode *)node;
      delset->Add(item);
#endif
      for(uint i = 0;i < rwset->elems;++i) {
        if((char *)(rwset->kvs[i].node) == node) {
          RWSet::RWSetItem &item = rwset->kvs[i];
          item.ro = false;
          delete item.addr;
          item.addr = NULL;
          item.len = 0;
#if TX_USE_LOG
          if(db_logger_){
            // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
            prepare_log(cor_id_, db_logger_, item);
          }
#endif  
          return;
        }
      }
      RWSet::RWSetItem item;

      item.tableid = tableid;
      item.key     = 0;
      item.len     = 0;
      item.node    = (MemNode *)node;
      item.addr    = NULL; // NUll means logical delete
      item.seq     = item.node->seq;
      item.ro      = false;
      rwset->Add(item);
#if TX_USE_LOG
      if(db_logger_){
        // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
        prepare_log(cor_id_, db_logger_, item);
      }
#endif 
    }

    void DBTX::delete_(int tableid,uint64_t key) {
#if 0
      MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);

      DeleteSet::DItem item;
      item.tableid = tableid;
      item.key = key;
      item.node = node;

      delset->Add(item);
#endif

      for(uint i = 0;i < rwset->elems;++i) {
        if(tableid == rwset->kvs[i].tableid &&
           rwset->kvs[i].key == key) {
          RWSet::RWSetItem &item = rwset->kvs[i];
          item.ro = false;
          delete item.addr;
          item.addr = NULL;
          item.len = 0;
#if TX_USE_LOG
          if(db_logger_){
            // printf("delete_ size:%d ,key:%lu\n",item.len, item.key);
            prepare_log(cor_id_, db_logger_, item);
          }
#endif
          return;
        }
      }

      MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
      RWSet::RWSetItem item;

      item.tableid = tableid;
      item.key     = key;
      item.len     = 0;
      item.node    = node;
      item.addr    = NULL; // NUll means logical delete
      item.seq     = node->seq;
      item.ro      = false;
      rwset->Add(item);
#if TX_USE_LOG
      if(db_logger_){
        // printf("delete_ size:%d ,key:%lu\n",item.len, item.key);
        prepare_log(cor_id_, db_logger_, item);
      }
#endif
    }

    void DBTX::write() {
      RWSet::RWSetItem &item = rwset->kvs[rwset->elems - 1];
      item.ro = false;
#if TX_USE_LOG
      if(db_logger_){
        // printf("write() size:%d\n",item.len);
        prepare_log(cor_id_, db_logger_, item);
      }
#endif
    }

    void DBTX::write(int tableid,uint64_t key,char *val,int len) {

      // first search the TX's r/w set
      for (uint i = 0; i < rwset->elems; i++) {
        if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
          RWSet::RWSetItem &item = rwset->kvs[i];
          item.ro = false;
#if TX_USE_LOG
          if(db_logger_){
            // printf("write(...) size:%d ,key:%lu\n",item.len, item.key);
            prepare_log(cor_id_, db_logger_, item);
          }
#endif
          //      return rwset->kvs[i].seq;
          //assert(rwset->kvs[0].tableid  != 0);
          return;
        }
      }

      fprintf(stderr,"@%d, local write operation not in the readset! tableid %d key %lu\n",
              thread_id,tableid,key);
      exit(-1);
    }

    void DBTX::local_ro_begin() {

      //if ( false == localinit) {
      //    readset = new ReadSet(thread_id);
      //    rwset = new RWSet();
      // local TX handler does not init remote-set
      //    localinit = true;
      //}
      ThreadLocalInit();
      readset->Reset();
      rwset->Reset();
      abort_ = false;
    }

    void DBTX::reset_temp() {
      readset->Reset();
      rwset->Reset();

    }

    void DBTX::_begin(DBLogger *db_logger,TXProfile *p ) {

      ThreadLocalInit();
      readset->Reset();
      rwset->Reset();
      remoteset->clear();
      //  delset->Reset();

#ifdef DEBUG
      profile = p;
#endif
      abort_ = false;

#if TX_USE_LOG
      db_logger_ = db_logger;
      //fprintf(stdout,"log begin at coroutine %d\n",cor_id_);
      if(db_logger_)db_logger->log_begin(cor_id_, 0);
#endif
    }

    int DBTX::add_to_remote_set(int tableid, uint64_t key, int pid) {
      return remoteset->add(REQ_READ,pid,tableid,key);
    }

    int  DBTX::add_to_remote_set(int tableid, uint64_t *key, int klen, int pid) {
      return remoteset->add(REQ_READ,pid,tableid,key,klen);
    }

    int DBTX::remote_read_idx(int tableid, uint64_t *key, int klen, int pid) {
      return remoteset->add(REQ_READ_IDX,pid,tableid,key,klen);
    }

    int DBTX::remote_insert(int tableid,uint64_t *key, int klen, int pid) {
      return remoteset->add(REQ_INSERT,pid,tableid,key,klen);
    }

    int DBTX::remote_insert_idx(int tableid, uint64_t *key, int klen, int pid) {
      return remoteset->add(REQ_INSERT_IDX,pid,tableid,key,klen);
    }


    void DBTX::remote_write(int tableid,uint64_t key,char *val,int len) {
      // TODO!!
    }

    void DBTX::remote_write(int r_idx,char *val,int len) {
#if TX_USE_LOG
      if(db_logger_){
        // printf("remote_write size: %d\n", len);
        RemoteSet::RemoteSetItem& item = remoteset->kvs_[r_idx];
        char* logger_val = db_logger_->get_log_entry(cor_id_, item.tableid, item.key, len, item.pid);
        memcpy(logger_val, val, len);
        db_logger_->close_entry(cor_id_, item.seq + 2);
      }
#endif
      remoteset->promote_to_write(r_idx,val,len);
    }

    void DBTX::do_remote_reads(yield_func_t &yield) {
      remoteset->do_reads(yield);
    }

    int  DBTX::do_remote_reads() {
      return remoteset->do_reads();
    }

    void DBTX::get_remote_results(int num_results) {
      remoteset->get_results(num_results);
      //remoteset->clear_for_reads();
    }

    bool DBTX::end_ro() {
#if OCC_RO_CHECK == 0
      return true;
#endif
#ifndef OCC_RETRY
      return true;
#endif
      if(abort_)
        return false;
      rwset->SetDBTX(this);

#if 1
      if(!readset->Validate()) {
        goto ABORT;
      }
#endif
      //assert(rwset->elems != 0);
      if (!rwset->CheckLocalSet()) {
#ifdef DEBUG
        profile->abortLocalValidate += 1;
#endif
        goto ABORT;
      }
      return true;
    ABORT:
      return false;
    }

    bool DBTX::debug_validate() {
      return readset->Validate();
    }

    int DBTX::report_rw_set() {
      return rwset->elems;
    }

    bool DBTX::end_fasst(yield_func_t &yield) {

      if(abort_) {
        return false;
      }
      rwset->SetDBTX(this);

#if 1
      if(unlikely(!rwset->LockAllSet()))  {
        //assert(false);
#ifdef DEBUG
        profile->abortLocalLock += 1;
#endif
        goto ABORT;
      }
#endif

#if 1
      if(!readset->Validate()) {
        goto ABORT;
      }
#endif

#if 1
      if (!rwset->CheckLocalSet()) {
#ifdef DEBUG
        profile->abortLocalValidate += 1;
#endif
        goto ABORT;
      }
#endif

#if TX_USE_LOG
      if(db_logger_){
        db_logger_->log_backups(cor_id_, timestamp);
        worker->indirect_must_yield(yield);
        db_logger_->log_end(cor_id_);
      }
#endif

      rwset->CommitLocalWrite();
      remoteset->commit_remote(yield);
      return true;

    ABORT:
#if TX_USE_LOG
      if(db_logger_){
        db_logger_->log_abort(cor_id_);
      }
#endif
      //remoteset->clear_for_reads();
      if(remoteset->elems_ > 0)
        remoteset->update_read_buf();

      remoteset->release_remote(yield);
      rwset->ReleaseAllSet();
      return false;
    }


    bool
    DBTX::end(yield_func_t &yield) {

#if TX_ONLY_EXE
      remoteset->update_write_buf();
      return true;
#endif
      if(abort_) {
        return false;
      }
      remoteset->need_validate_ = true; // let remoteset to send the validate
      rwset->SetDBTX(this);

      if(!remoteset->lock_remote(yield)) {
#if !NO_EXE_ABORT
        goto ABORT;
#endif
      }
#if 1
      if(unlikely(!rwset->LockAllSet()))  {
        //assert(false);
#ifdef DEBUG
        profile->abortLocalLock += 1;
#endif
#if !NO_EXE_ABORT
        goto ABORT;
#endif
      }
#endif

#ifdef OCC_RETRY
      if(!remoteset->validate_remote(yield)) {
#if !NO_EXE_ABORT
        goto ABORT;
#endif
      }
#else
      //remoteset->clear_for_reads();
#endif

#if 0
      if(!readset->Validate()) {
        goto ABORT;
      }
#endif

#if 0
      if (!rwset->CheckLocalSet()) {
#ifdef DEBUG
        profile->abortLocalValidate += 1;
#endif
#if !NO_EXE_ABORT
        goto ABORT;
#endif
      }
#endif

#if TX_USE_LOG
      if(db_logger_){
        db_logger_->log_backups(cor_id_, timestamp);
        worker->indirect_must_yield(yield);
        db_logger_->log_end(cor_id_);
      }

#endif
      rwset->CommitLocalWrite();
#if COMMIT_NAIVE
      remoteset->commit_remote_naive(yield);
#else
      remoteset->commit_remote(yield);
#endif
      return true;
    ABORT:
      //  assert(false);
      //  fprintf(stdout,"abort @%d\n",thread_id);
#if TX_USE_LOG
      if(db_logger_){
        db_logger_->log_abort(cor_id_);
      }
#endif
      //remoteset->clear_for_reads();
      remoteset->release_remote(yield);
      rwset->ReleaseAllSet();
      //rwset->GC();
      return false;
    }

    void
    DBTX::abort() {
#if TX_USE_LOG
      if(db_logger_){
        db_logger_->log_abort(cor_id_);
      }
#endif
    }

    DBTXIterator::DBTXIterator(DBTX* tx, int tableid,bool sec) : tableid(tableid)
    {
      tx_ = tx;
      if(sec) {
        iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
      } else {
        iter_ = (tx->txdb_->stores_[tableid])->GetIterator();
      }
      cur_ = NULL;
      prev_link = NULL;
    }


    bool DBTXIterator::Valid()
    {
      return cur_ != NULL && cur_->value != NULL;
    }


    uint64_t DBTXIterator::Key()
    {
      return iter_->Key();
    }

    char* DBTXIterator::Value()
    {
      return (char *)val_;
    }

    char* DBTXIterator::Node() {
      return (char *)cur_;
    }

    void DBTXIterator::Next()
    {
      bool r = iter_->Next();

      while(iter_->Valid()) {

        cur_ = iter_->CurNode();
        {

          RTMScope rtm(NULL);
          val_ = cur_->value;

          if(prev_link != iter_->GetLink()) {
            tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
            prev_link = iter_->GetLink();
          }
          tx_->readset->Add(&cur_->seq);

          if(DBTX::ValidateValue(val_)) {
            //	fprintf(stdout,"normal case return\n");
            return;
          }

        }
        iter_->Next();
      }
      cur_ = NULL;
    }

    void DBTXIterator::Prev()
    {
      bool b = iter_->Prev();
      if (!b) {
        //    tx_->abort = true;
        cur_ = NULL;
        return;
      }

      while(iter_->Valid()) {

        cur_ = iter_->CurNode();
        {

          RTMScope rtm(NULL);
          val_ = cur_->value;

          tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
          tx_->readset->Add(&cur_->seq);

          if(DBTX::ValidateValue(val_)) {
            return;
          }
        }
        iter_->Prev();
      }
      cur_ = NULL;
    }

    void DBTXIterator::Seek(uint64_t key)
    {
      //Should seek from the previous node and put it into the readset
      iter_->Seek(key);
      cur_ = iter_->CurNode();

      //No keys is equal or larger than key
      if(!iter_->Valid()){

        assert(cur_ == NULL);
        RTMScope rtm(NULL);

        //put the previous node's next field into the readset

        //printf("Not Valid!\n");
        //sleep(20);
        //assert(false);
        //ASSERT_PRINT(false,stderr,"Tableid %d\n",tableid);
        tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
        return;
      }

      //Second, find the first key which value is not NULL
      while(iter_->Valid()) {

        {
          RTMScope rtm(NULL);
          //Avoid concurrently insertion
          tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
          //	  printf("end\n");
          //Avoid concurrently modification
          //      tx_->readset->Add(&cur_->seq);
          val_ = cur_->value;

          if(DBTX::ValidateValue(val_)) {
            return;
          }
          //assert(false);
        }

        iter_->Next();
        cur_ = iter_->CurNode();
      }
      cur_ = NULL;
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void DBTXIterator::SeekToFirst()
    {
      //Put the head into the read set first

      iter_->SeekToFirst();

      cur_ = iter_->CurNode();

      if(!iter_->Valid()) {
        assert(cur_ == NULL);
        RTMScope rtm(NULL);

        //put the previous node's next field into the readset

        tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());


        return;
      }

      while(iter_->Valid()) {
        {

          RTMScope rtm(NULL);
          val_ = cur_->value;

          tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
          tx_->readset->Add(&cur_->seq);

          if(DBTX::ValidateValue(val_)) {
            return;
          }

        }

        iter_->Next();
        cur_ = iter_->CurNode();
      }
      cur_ = NULL;
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void DBTXIterator::SeekToLast()
    {
      assert(0);
    }


    DBTXTempIterator::DBTXTempIterator (DBTX *tx,int tableid,bool sec) {
      tx_ = tx;
      if(sec) {
        iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
      } else
        iter_ = (tx_->txdb_->stores_[tableid])->GetIterator();
      cur_ = NULL;
      prev_link = NULL;
    }


    bool DBTXTempIterator::Valid() {
      /* Read will directly read from the latest one */
      //  if(cur_ != NULL)
      //    assert(cur_->seq != 0);
      return cur_ != NULL && val_ != NULL;
    }

    uint64_t DBTXTempIterator::Key()
    {
      return iter_->Key();
    }

    char *DBTXTempIterator::Value() {
      return (char *)val_ ;
    }

    char *DBTXTempIterator::Node() {
      return (char *)cur_;
    }

    void DBTXTempIterator::Next() {
      bool r = iter_->Next();
      //    assert(r);
      while(iter_->Valid()) {
        cur_ = iter_->CurNode();
        {
          RTMScope rtm(NULL);
          val_ = cur_->value;
          if(prev_link != iter_->GetLink() ) {
            prev_link = iter_->GetLink();
          }
          if(ValidateValue(val_) )
            return;
        }
        iter_->Next();
      }
      cur_ = NULL;
    }

    void DBTXTempIterator::Prev() {

      bool b = iter_->Prev();
      if(!b) {
        //      tx_->abort = true;
        cur_ = NULL;
        return;
      }

      while(iter_->Valid()) {
        cur_ = iter_->CurNode();
        {
          RTMScope rtm(NULL);
          val_ = cur_->value;
          if(ValidateValue(val_))
            return;
        }
        iter_->Prev();
      }
      cur_ = NULL;
    }

    void DBTXTempIterator::Seek(uint64_t key) {

      iter_->Seek(key);
      cur_ = iter_->CurNode();

      if(!iter_->Valid()) {
        assert(cur_ == NULL) ;
        //    fprintf(stderr,"seek fail..\n");
        return ;
      }

      while (iter_->Valid()) {
        {
          RTMScope rtm(NULL) ;
          val_ = cur_->value;
          if(ValidateValue(val_)) {
            //	fprintf(stdout,"one time succeed\n");
#if 0
            if(!Valid()) {
              fprintf(stderr,"one time error!\n");
              exit(-1);
            }
#endif
            return;
          }
        }
        iter_->Next();
        cur_ = iter_->CurNode();

      }

      cur_ = NULL;
    }

    void DBTXTempIterator::SeekToFirst() {
      /* TODO ,not implemented. seems not needed */
    }

    void DBTXTempIterator::SeekToLast() {
      /* TODO ,not implemented */
    }

  }
}
