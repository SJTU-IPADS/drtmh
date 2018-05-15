#include "all.h"
#include "tx_config.h"

#include "global_config.h"
#include "./db/config.h"

#include "framework/bench_worker.hpp"
#include "framework/req_buf_allocator.h"

#include <unistd.h>
#include <algorithm>

#include <functional> // for std::bind

#include "db_farm.h"

#define MAXSIZE 1024
#define META_LENGTH 16

extern size_t nthreads; // number of total worker threads in one node
extern size_t current_partition; // current partition-id

namespace nocc {

  extern __thread BenchWorker* worker;
  namespace oltp {
    extern __thread oltp::RPCMemAllocator *msg_buf_alloctors;
  }
  using namespace oltp;

#if LOCAL_LOCK_USE_RDMA
  const uint64_t worker_space = 40 * 1024 * 1024;
  char* rdma_buf = NULL;
#endif
  namespace db {

    // first is the rwset operations //////////////////////////////////////////
    class DBFarm::RWSet  {
    public:
      struct RWSetItem {
        int32_t tableid;
        uint64_t key;
        int len;
        MemNode  *node;
        uint64_t *addr;  // Pointer to the new value buffer
        uint64_t seq;    // local buffered seq for validation
        bool ro;
      };

      int max_length;
      RWSetItem *kvs;    // true rwset
      int elems;         // total elements in the set
      int current;       // number of locked object
      int cor_id;        // coroutine number
      int tid;           // thread id

#if LOCAL_LOCK_USE_RDMA
      DBFarm *context_;
      void set_context(DBFarm *context) { context_ = context; }
#endif

      RWSet(int tid,int cid);
      ~RWSet() {  delete [] kvs; }

      inline void reset()  { elems = 0; current = -1;}
      inline void resize() { // expand the read-write set if necessary
        max_length = max_length * 2;
        RWSetItem *nkvs = new RWSetItem[max_length];
        for (int i = 0; i < elems; i++) {
          nkvs[i] = kvs[i];
        }
        delete []kvs;
        kvs = nkvs;
      }

      inline void add(RWSetItem &item);

      // commit helper functions
      inline bool lock_all_set();
      inline void release_all_set();
      inline bool check_local_set(); // validation

      inline int  commit_local_write();

      inline void gc() {  for(uint i = 0;i < elems;++i) { free(kvs[i].addr);} }

      bool inline is_locked(uint64_t *ptr) {
        return (*ptr) != 0;
      }
    };

    DBFarm::RWSet::RWSet(int tid,int cid): tid(tid),cor_id(cid)
    {
      max_length = MAXSIZE;
      elems = 0;
      kvs = new RWSetItem[max_length];

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

    inline void DBFarm::RWSet::add(RWSetItem &item) {

      if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
      if (elems == max_length) {
        resize();
      }
      int cur = elems;
      elems++;

      kvs[cur].tableid = item.tableid;
      kvs[cur].key = item.key;
      kvs[cur].len = item.len;
      kvs[cur].ro = item.ro;

      kvs[cur].seq = item.seq;
      kvs[cur].addr = item.addr;
      kvs[cur].node = item.node;
    }

    inline bool DBFarm::RWSet::lock_all_set() {

#if LOCAL_LOCK_USE_RDMA
      char* local_buf = (char*)Rmalloc(sizeof(uint64_t));
#endif

      for(int i = 0;i < elems;++i) {
        if(kvs[i].ro) {
          continue;
        }

        //volatile uint64_t *lockptr = &(kvs[i].node->lock);
        volatile uint64_t *lockptr = (volatile uint64_t *)(kvs[i].node->value);
        uint64_t lock_content = ENCODE_LOCK_CONTENT(current_partition,tid,cor_id + 1);

#if LOCAL_LOCK_USE_RDMA
        uint64_t offset = context_->random_[context_->cor_id_].next() % (worker_space - sizeof(uint64_t));
        offset = offset + 64 - offset % 64;
        offset = (uint64_t)rdma_buf + offset - (uint64_t)context_->base_ptr_;
        context_->local_qp_->rc_post_compare_and_swap(local_buf,0,0, lock_content, IBV_SEND_SIGNALED);
        context_->local_qp_->poll_completion();
        if(unlikely(*((uint64_t*)local_buf) != 0)) {
          Rfree(local_buf);
          return false;
        }
#else
        if( unlikely( (*lockptr != 0) ||
                      !__sync_bool_compare_and_swap(lockptr,0,lock_content))) {
          return false;
        }
#endif
        current = i;
      }

#if LOCAL_LOCK_USE_RDMA
      Rfree(local_buf);
#endif

      return true;
    }

    inline void DBFarm::RWSet::release_all_set() {

#if LOCAL_LOCK_USE_RDMA
      char* local_buf = (char*)Rmalloc(sizeof(uint64_t));
#endif

      for(int i = 0;i <= current;++i) {
        if(kvs[i].ro)
          continue;
        //volatile uint64_t *lockptr = &(kvs[i].node->lock);
        volatile uint64_t *lockptr = (volatile uint64_t *)(kvs[i].node->value);
        uint64_t lock_content = ENCODE_LOCK_CONTENT(current_partition,tid,cor_id + 1);

#if LOCAL_LOCK_USE_RDMA
        uint64_t offset = context_->random_[context_->cor_id_].next() % (worker_space - sizeof(uint64_t));
        offset = offset + 64 - offset % 64;

        context_->local_qp_->rc_post_compare_and_swap(local_buf,
              (uint64_t)rdma_buf + offset - (uint64_t)context_->base_ptr_,lock_content,0, IBV_SEND_SIGNALED);
        context_->local_qp_->poll_completion();
#else
        __sync_bool_compare_and_swap(lockptr,lock_content,0);
#endif
      }

#if LOCAL_LOCK_USE_RDMA
      Rfree(local_buf);
#endif

    }

    inline bool DBFarm::RWSet::check_local_set() {

      for(uint i = 0;i < elems;++i) {
        RRWSet::Meta *m = (RRWSet::Meta *)(kvs[i].node->value);
        auto seq = m->seq;
        if(seq != kvs[i].seq) {
          //          assert(false);
          return false;
        }
      }
      return true;
    }

    inline int DBFarm::RWSet::commit_local_write() {

      int counter = 0;
      for(uint i = 0;i < elems;++i) {
        if(kvs[i].ro)
          continue;
        /* local writes */
        RRWSet::Meta *m = (RRWSet::Meta *)(kvs[i].node->value);
        auto old_seq = m->seq;
        m->seq = 1;
#if 1
        asm volatile("" ::: "memory");
        //kvs[i].node->value = kvs[i].addr;
        memcpy((char *)(kvs[i].node->value) + META_LENGTH,(char *)(kvs[i].addr) + META_LENGTH,kvs[i].len);
        asm volatile("" ::: "memory");
#endif
        m->seq = old_seq + 2;
        /* By the way, release the lock */
        asm volatile("" ::: "memory");
        m->lock = 0;
        //kvs[i].node->lock = 0;
#if 0
        if(kvs[i].tableid == 1) {
          fprintf(stdout,"seq %lu key %lu\n",old_seq,kvs[i].key);
          sleep(1);
        }
#endif
      }
      return counter;
    }

    // comment ////////////////////////////////////////////////////////////////


    // Iterator implementations  //////////////////////////////////////////////

    DBFarmIterator::DBFarmIterator (DBFarm *tx,int tableid,bool sec) {
      tx_ = tx;
      if(sec) {
        // create index from secondary index
        iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
      } else
        iter_ = (tx_->txdb_->stores_[tableid])->GetIterator();
      cur_ = NULL;
      prev_link = NULL;
    }

    bool DBFarmIterator::Valid() {
      /* Read will directly read from the latest one */
      return cur_ != NULL && val_ != NULL;
    }

    uint64_t DBFarmIterator::Key(){  return iter_->Key();}
    char *DBFarmIterator::Value() { return (char *)val_ ;}
    char *DBFarmIterator::Node()  { return (char *)cur_; }

    void DBFarmIterator::Next() {
      bool r = iter_->Next();
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

    void DBFarmIterator::Prev() {

      bool b = iter_->Prev();
      if(!b) {
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

    void DBFarmIterator::Seek(uint64_t key) {
      iter_->Seek(key);
      cur_ = iter_->CurNode();

      if(!iter_->Valid()) {
        assert(cur_ == NULL) ;
        return ;
      }

      while (iter_->Valid()) {
        {
          RTMScope rtm(NULL) ;
          val_ = cur_->value;
          if(ValidateValue(val_)) {
            return;
          }
        }
        iter_->Next();
        cur_ = iter_->CurNode();
      }
      cur_ = NULL;
    }

    void DBFarmIterator::SeekToFirst() { /* TODO ,not implemented. seems not needed */}

    void DBFarmIterator::SeekToLast() {/* TODO ,not implemented */}

    // comment ////////////////////////////////////////////////////////////////


    // Real implementation of FaRM's OCC //////////////////////////////////////
    DBFarm::DBFarm(rdmaio::RdmaCtrl *cm,RDMA_sched *sched,MemDB *tables,int t_id,RRpc *rpc,int c_id)
      :lock_buf_(NULL),reply_buf_(NULL),
       commit_buf_(NULL),                 // init of msg buffer
       read_server_num_(0),               // number of server to read
       write_server_num_(0),              // number of server to write
       write_items_(0),                   // number of items intend to write
       localinit(false),                  // whether the handler has been inited or not
       TXHandler(c_id),                   // init coroutine id
       txdb_(tables),thread_id(t_id),rpc_handler_(rpc),cm_(cm), // related data structures
       base_ptr_((char *)(cm->conn_buf_)), // start address of RDMA to calculate the off
       sched_(sched)
    {
      server_set_.clear();

      // register RPC handlers
      using namespace  std::placeholders;
      rpc_handler_->register_callback(std::bind(&DBFarm::lock_rpc_handler,this,_1,_2,_3,_4),RPC_LOCK,true);
      rpc_handler_->register_callback(std::bind(&DBFarm::release_rpc_handler,this,_1,_2,_3,_4),
                                      RPC_RELEASE,true);
#if ONE_WRITE == 1
      // use one-sided write does not require register the commit RPC handler
#else
      rpc_handler_->register_callback(std::bind(&DBFarm::commit_rpc_handler,this,_1,_2,_3,_4),
                                      RPC_COMMIT,true);
#endif

      // some simple checks
      assert(META_LENGTH == sizeof(RRWSet::Meta));

      // then init statics number
      INIT_LAT_VARS(lock);
    }

    void DBFarm::thread_local_init() {

      if(false == localinit) {

        rwset_     = new RWSet(thread_id,cor_id_);
#if LOCAL_LOCK_USE_RDMA
        if(thread_id == 0){
          rdma_buf = (char*)Rmalloc(worker_space);
          assert(rdma_buf != NULL);
        }
        random_ = new util::fast_random[ 1 + worker->server_routine];
        for(uint i = 0;i < 1 + worker->server_routine;++i){
          random_[i].set_seed0(worker->rand_generator_.next());
        } 
        local_qp_ = cm_->get_rc_qp(thread_id,current_partition,2);
        rwset_->set_context(this);
#endif
        rrwset_    = new RRWSet(cm_,sched_,txdb_,thread_id,cor_id_,META_LENGTH);

        lock_buf_   = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
        commit_buf_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
        reply_buf_ = (char *)malloc(MAX_MSG_SIZE);

        localinit = true;
      } // init done
    }

    void DBFarm::_begin(DBLogger *db_logger,TXProfile *p) {
      // init
      abort_ = false;
      thread_local_init();

      lock_buf_end_   = lock_buf_ + sizeof(ReqHeader);  // zeroing the msg
      commit_buf_end_ = commit_buf_ + sizeof(ReqHeader);

      write_items_ = 0;
      write_server_num_ = 0;
      server_set_.clear();

      // clear rwsets
      rwset_->reset();
      rrwset_->clear();

#if USE_LOGGER
      db_logger_ = db_logger;
      if(db_logger_)db_logger->log_begin(cor_id_, 0);
#endif
    }

    void DBFarm::local_ro_begin() {
      thread_local_init();
      rwset_->reset();
      remoteset->clear();
      abort_ = false;
    }

    bool DBFarm::end(yield_func_t &yield) {
#if TX_ONLY_EXE
      return true;
#endif

      if(abort_) return false;
#if 1
      if(!lock_remote(yield)) {
#if !NO_EXE_ABORT
    goto ABORT;
#endif
      }
#endif

#if 1
      if(!validate_remote(yield)){
#if !NO_EXE_ABORT
    goto ABORT;
#endif
      }
#endif

#if 1
      if(!rwset_->lock_all_set()) {
#if !NO_EXE_ABORT
    goto ABORT;
#endif
      }
      if(!rwset_->check_local_set()){
#if !NO_EXE_ABORT
    goto ABORT;
#endif
      }

#if USE_LOGGER
      if(db_logger_){
        db_logger_->log_backups(cor_id_);
        worker->indirect_must_yield(yield);
        db_logger_->log_end(cor_id_);
      }
#endif
      rwset_->commit_local_write(); // commit local writes
#endif

      commit_remote(); // commit remote writes
      return true;
    ABORT:
#if USE_LOGGER
      if(db_logger_){
        db_logger_->log_abort(cor_id_);
      }
#endif
      release_remote();
#if 1
      rwset_->release_all_set();
#endif
      return false;
    }

    void DBFarm::abort() {
#if USE_LOGGER
      if(db_logger_){
        db_logger_->log_abort(cor_id_);
      }
#endif
      abort_ = true; 
    }

    // local gets
    uint64_t DBFarm::get(int tableid,uint64_t key,char **val,int len) {

      char *_addr = (char *)malloc(len + META_LENGTH);
      MemNode *node = NULL;
      node = txdb_->stores_[tableid]->GetWithInsert(key);

    retry:
      if(unlikely(node->value == NULL)){
        fprintf(stderr,"get error ,tableid %d, key %lu\n",tableid,key);
        assert(false);
        return 1;
      }
      RRWSet::Meta *m = (RRWSet::Meta *)(node->value);
      uint64_t seq = m->seq;

      asm volatile("" ::: "memory");
      if(unlikely(seq == 1)) { goto retry;}
      uint64_t *tmpVal = node->value;

      asm volatile("" ::: "memory");
      memcpy(_addr + META_LENGTH,(char *)tmpVal + META_LENGTH,len);
      asm volatile("" ::: "memory");
      if( unlikely(m->seq != seq) ) {
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
      item.node = (MemNode *)node;

      rwset_->add(item);
#endif
      *val = ((char *)_addr + META_LENGTH);
      return seq;
    }

#if USE_LOGGER
    inline void prepare_log(int cor_id, DBLogger* db_logger, const DBFarm::RWSet::RWSetItem& item){
        char* val = db_logger->get_log_entry(cor_id, item.tableid, item.key, item.len);
        // printf("%p %p %d %lu\n", val, (char*)item.addr + META_LENGTH, item.len, item.key);
        memcpy(val, (char*)item.addr + META_LENGTH, item.len);
        db_logger->close_entry(cor_id);
    }
#endif
    void DBFarm::write(int tableid,uint64_t key,char *val,int len) {
      // first search the TX's r/w set
      for (uint i = 0; i < rwset_->elems; i++) {
        if (rwset_->kvs[i].tableid == tableid && rwset_->kvs[i].key == key) {
          RWSet::RWSetItem &item = rwset_->kvs[i];
          item.ro = false;
#if USE_LOGGER
          if(db_logger_){
            prepare_log(cor_id_, db_logger_, item);
          }
#endif
          return;
        }
      }
    }

    uint64_t DBFarm::get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield) {
      char *temp;
      int len = txdb_->_schemas[tableid].vlen;
      auto res = get(tableid,key,&temp,len);
      memcpy(val,temp,len);
      return res;
    }

    void DBFarm::write() {
      RWSet::RWSetItem &item = rwset_->kvs[rwset_->elems - 1];
      item.ro = false;
#if USE_LOGGER
      if(db_logger_){
        prepare_log(cor_id_, db_logger_, item);
      }
#endif
    }

    // local inserts
    void DBFarm::insert(int tableid,uint64_t key,char *val,int len) {

      RWSet::RWSetItem item;

      int vlen = META_LENGTH + len;

      // round up
      vlen = vlen + CACHE_LINE_SZ - vlen % CACHE_LINE_SZ;

      item.addr = (uint64_t *) (new char[vlen]);
      memset(item.addr,0,META_LENGTH);
      memcpy( (char *)item.addr + META_LENGTH, val,len);

    retry:
      MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key,(char *)(item.addr));

      item.tableid = tableid;
      item.key = key;
      item.seq = 0;
      item.len = len;
      item.ro = false;
      item.node = node;

      rwset_->add(item);
#if USE_LOGGER
      if(db_logger_){
        prepare_log(cor_id_, db_logger_, item);
      }
#endif
    }

    void DBFarm::insert_index(int idx_id,uint64_t key,char *val) {
      MemNode *node = txdb_->_indexs[idx_id]->GetWithInsert(key);
      RWSet::RWSetItem item;
      item.tableid = idx_id;
      item.key     = key;
      item.node    = node;
      item.addr    = (uint64_t *)val;
      item.seq     = node->seq;
      item.ro      = false;
      rwset_->add(item);
    }

    void DBFarm::delete_(int tableid,uint64_t key) {
      for(uint i = 0;i < rwset_->elems;++i) {
        if(tableid == rwset_->kvs[i].tableid && rwset_->kvs[i].key == key) {
          RWSet::RWSetItem &item = rwset_->kvs[i];
          item.ro = false;
          delete item.addr;
          item.addr = NULL;
          item.len = 0;
#if USE_LOGGER
          if(db_logger_){
            prepare_log(cor_id_, db_logger_, item);
          }
#endif
          return;
        }
      }
      // end delete function
    }

    // remote operators

    // fetch remote records

    int DBFarm::add_to_remote_set(int tableid,uint64_t key,int pid,yield_func_t &yield) {
      int vlen = txdb_->_schemas[tableid].vlen;
      return rrwset_->add(pid,tableid,key,vlen + META_LENGTH,yield);
    }

    int DBFarm::add_to_remote_set(int tableid,uint64_t key,int pid) {
      int vlen = txdb_->_schemas[tableid].vlen;
      return rrwset_->add(pid,tableid,key,vlen + META_LENGTH);
    }

    int DBFarm::add_to_remote_set_imm(int tableid,uint64_t key,int pid) {
      return add_to_remote_set(tableid,key,pid);
    }

    // get the cached value of remote values
    uint64_t DBFarm::get_cached(int idx,char **val) {
      assert(idx < rrwset_->numItems());

      RRWSet::Meta *m = (RRWSet::Meta *)(rrwset_->kvs_[idx].val);
      *val = (char *)((char *)(rrwset_->kvs_[idx].val) + META_LENGTH);

      return m->seq;
    }

    void  DBFarm::get_imm_res(int idx,char **ptr,int size) {
      // direct fetch result from one-sided-remote set
      assert(idx < rrwset_->numItems());
      *ptr = (char *)(rrwset_->kvs_[idx].val + META_LENGTH);
    }

    uint64_t DBFarm::get_cached(int tableid,uint64_t key,char **val) {
      for(uint i = 0;i < rrwset_->numItems(); ++i) {
        if(rrwset_->kvs_[i].tableid == tableid && rrwset_->kvs_[i].key == key) {

          RRWSet::Meta *m = (RRWSet::Meta *)(rrwset_->kvs_[i].val);
          *val = (char *)(rrwset_->kvs_[i].val + META_LENGTH);
          return m->seq;
        }
      }
      return 0;
    }

    void DBFarm::do_remote_reads(yield_func_t &yield) {assert(false);}

    int  DBFarm::do_remote_reads() {
      // TODO
      return 0;
    }


    void DBFarm::report() {
      REPORT(lock);
      if(rrwset_)
        rrwset_->report();
    }
    // comment ////////////////////////////////////////////////////////////////

  }; // end namespace db
}; // end namespace nocc
