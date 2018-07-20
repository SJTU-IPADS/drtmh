#include "all.h"

#include "rocc_config.h"
#include "tx_config.h"

#include "rdmaio.h"
#include "ralloc.h"

#include "tx_config.h"

#include "framework/bench_worker.h"

#include <time.h>
#include <sys/time.h>

#include "dbsi.h"

using namespace nocc::db;
using namespace nocc::oltp;

using namespace rdmaio;

__thread uint64_t  local_seconds;

extern size_t current_partition ;
extern size_t nthreads;
extern size_t total_partition;

#define MAX_SIZE 1024

#define MAX(x,y) (  (x) > (y) ? (x) : (y))

#define THREAD_ID_OFFSET (54)
#define TID_TS_OFFSET (0)
#define SECOND_OFFSET (22)
#define SECOND_MASK (0xffffffff)

/* The counter is carefully choosn so that increasing it will not overlap second */
#define COUNTER_MASK (0x3fffff)
#define THREAD_ID_MASK (0x3ff)
#define TIMESTAMP_MASK (0x3fffffffffffff)

#define GEN_TIME(sec,counter) (  ((sec) << SECOND_OFFSET) | (counter) )
#define GEN_TID(tid,time) ( ((tid << THREAD_ID_OFFSET ) | (time) ) )
#define GET_TIMESTAMP(tid) ( (tid) & TIMESTAMP_MASK)
#define GET_TID(tid) ( (tid) >> THREAD_ID_OFFSET)
#define GET_SEC(tid) ( ((tid) & TIMESTAMP_MASK) >> SECOND_OFFSET)

namespace nocc {

extern __thread BenchWorker* worker;

namespace oltp {
extern __thread oltp::RPCMemAllocator *msg_buf_alloctors;
}
using namespace oltp;

extern RdmaCtrl *cm;

namespace db {

extern __thread coroutine_func_t *routines_;

TSManager *ts_manager;
#if LARGE_VEC == 1
__thread uint64_t TSManager::local_ts = 0;
#else
uint64_t TSManager::local_ts = 0;
uint64_t TSManager::last_ts  = 0;
#endif

void DBSI::GlobalInit() {

}

int SIGetMetalen() {
    return SI_META_LEN;
}

/* Write set's definiation */
class DBSI::WriteSet {

  public:
    struct  WriteSetItem {
        int32_t tableid;
        uint64_t key;
        int len ;
        MemNode  *node;
        uint64_t *addr;  /* Pointer to the new value buffer */
        uint64_t seq;
        bool ro;
    };

    DBSI *tx_;
    int max_len;
    WriteSetItem *kvs;

    /* How many items in write set*/
    int elems;
    /* How many items which has been locked */
    int current;

    WriteSet();
    ~WriteSet();

    inline void Reset();
    inline void SetTX(DBSI *dbtx);
    inline void Add(WriteSetItem &item);

    /* commit helper functions */
    inline bool LockAllSet(uint64_t );
    inline void ReleaseAllSet();

    inline int  CommitLocalWrite(uint64_t commit_ts);

    inline void GC();

    bool inline IsLocked(uint64_t *ptr) {
        return (*ptr) != 0;
    }
};

DBSI::WriteSet::WriteSet () {
    max_len = MAX_SIZE;
    elems = 0;
    kvs   = new WriteSetItem[max_len];
    tx_   = NULL;

    for(uint i = 0;i < max_len;++i) {
        memset(&(kvs[i]),0,sizeof(WriteSetItem));
    }
}

DBSI::WriteSet :: ~WriteSet() {
    delete []kvs;
    kvs = NULL;
}

inline void DBSI::WriteSet::GC () {
    for(uint i = 0;i < elems;++i) {
        if(kvs[i].addr != NULL)
            free(kvs[i].addr);
    }
}

inline void DBSI::WriteSet::Add(WriteSetItem &item) {
    if(max_len < elems) printf("ELEMS %d MAX %d\n",elems, max_len);
    if(elems == max_len) assert(false);

    int cur = elems;
    elems ++;
    kvs[cur].tableid = item.tableid;
    kvs[cur].key = item.key;
    kvs[cur].len = item.len;
    kvs[cur].node = item.node;
    kvs[cur].addr = item.addr;
    kvs[cur].seq  = item.seq;
    kvs[cur].ro   = item.ro;
}


inline void DBSI::WriteSet::SetTX(DBSI *dbtx) {
    tx_ = dbtx;
}

inline void DBSI::WriteSet::Reset() {
    elems = 0;
    current = -1;
}

inline bool DBSI::WriteSet::LockAllSet(uint64_t commit_ts) {

    for(uint i = 0;i < elems;++i) {
        //    fprintf(stdout,"tab %d\n",kvs[i].tableid);
        /* Lock + check */
        if(kvs[i].ro )
            continue;
        volatile uint64_t *lockptr = &(kvs[i].node->lock);

        if( unlikely( (*lockptr != 0) ||
                      !__sync_bool_compare_and_swap(lockptr,0,
                                                    ENCODE_LOCK_CONTENT(current_partition,tx_->thread_id,
                                                                        tx_->cor_id_ + 1)))) {

            return false;
        }
        /* lock process to i */
        current = i;
#if 1
        /* it exceed my timestamp */
#ifdef SI_VEC
        if(kvs[i].node->seq != kvs[i].seq) {
            //fprintf(stdout,"desired %lu, real %lu\n",kvs[i].seq,kvs[i].node->seq);
            //sleep(1);
            return false;
        }
#else
        assert( SI_GET_COUNTER(kvs[i].node->seq) <= ts_buffer_[SI_GET_SERVER(kvs[i].node->seq)]);
        if( kvs[i].node->seq > tx_->ts_buffer_[0])
            return false;
#endif
#else

        if(kvs[i].node->seq != kvs[i].seq) {
            return false;
        }
#endif
    }
    return true;
}

inline void DBSI::WriteSet::ReleaseAllSet() {
    /* This shall be int, since current is -1 if no lock succeed, uint will cause an overvlow */
    for(int i = 0;i <= current;++i) {
        if(kvs[i].ro)
            continue;
        kvs[i].node->lock = 0;
    }
}


inline int DBSI::WriteSet::CommitLocalWrite(uint64_t commit_ts) {

#if RECORD_STALE
    auto time = std::chrono::system_clock::now();
#endif

    for(uint i = 0;i < elems;++i) {
        if(kvs[i].ro)
            continue;
        /* Forbiding concurrent read access */
        kvs[i].node->seq = 1;
        //fprintf(stdout,"write record %p\n",kvs[i].node);
        asm volatile("" ::: "memory");
#if 1
        uint64_t *cur    = kvs[i].node->value;
        /* Using value switch */
        uint64_t *oldptr = kvs[i].node->old_value;

        if(likely(cur != NULL)) {
            _SIValHeader * hptr = (_SIValHeader *)cur;
            hptr->oldValue = oldptr;
            hptr->version  = kvs[i].seq;
            if(unlikely(kvs[i].seq == 0)) {
                fprintf(stdout,"tableid %d\n",kvs[i].tableid);
                assert(false);
            }
#if RECORD_STALE
            hptr->time     = kvs[i].node->time;
#endif
            assert(hptr->version != 0);
        } else {
            // insertion
            //assert(oldptr == NULL);
            kvs[i].node->value = kvs[i].addr;
        }
        /* TODO, may set the version */
        kvs[i].node->old_value = cur;
        kvs[i].node->value = kvs[i].addr;
        // TODO, shall be set to commit version
#else
        memcpy( (char *)cur + SI_META_LEN, (char *)(kvs[i].addr) + SI_META_LEN, kvs[i].len);
#endif
        asm volatile("" ::: "memory");
#ifndef EM_OCC
        kvs[i].node->seq = commit_ts;
#else
        kvs[i].node->seq = kvs[i].seq + 2;
#endif
#if RECORD_STALE
        assert(kvs[i].node->time <= time);
        kvs[i].node->time = time;
#endif
        //      assert(commit_ts > kvs[i].seq);
        asm volatile("" ::: "memory");
        /* Release the lock*/
        kvs[i].node->lock = 0;
    }
    return 0;
}


DBSI::DBSI(MemDB *tables,int t_id,RRpc *rpc,int c_id)
        : txdb_(tables), thread_id(t_id),rpc_(rpc),
          abort_(false),
          TXHandler(c_id) // farther class
{
    // register rpc handlers
    using namespace std::placeholders;
    rpc_->register_callback(std::bind(&DBSI::get_rpc_handler,this,_1,_2,_3,_4),RPC_READ,true);
    rpc_->register_callback(std::bind(&DBSI::lock_rpc_handler,this,_1,_2,_3,_4),RPC_LOCK,true);
    rpc_->register_callback(std::bind(&DBSI::release_rpc_handler,this,_1,_2,_3,_4),RPC_RELEASE,true);
    rpc_->register_callback(std::bind(&DBSI::commit_rpc_handler2,this,_1,_2,_3,_4),RPC_COMMIT,true);
    rpc_->register_callback(std::bind(&DBSI::acquire_ts_handler,this,_1,_2,_3,_4),RPC_TS_ACQUIRE,true);
#if TS_USE_MSG == 1
    rpc_->register_callback(std::bind(&TSManager::ts_update_handler,ts_manager,
                                      _1,_2,_3,_4),RPC_COMMIT + 1,true);
#endif

#if USE_RDMA
    // get the QP vector
    for(uint i = 0;i < cm->get_num_nodes();++i) {
        auto qp = cm->get_rc_qp(thread_id,i,1); // use QP at idx 1
        assert(qp != NULL);
        qp_vec_.push_back(qp);
    }
#endif
    // update some local variables
    TXHandler::nreadro_locked = 0;
    localinit = false;
}

inline uint64_t
DBSI::_get_ro_versioned_helper(int tableid, uint64_t key, char *val, uint64_t version,yield_func_t &yield) {
    //#ifdef EM_OCC
#if 0
    {
        MemNode *node = NULL;
        int len = txdb_->_schemas[tableid].vlen;
        node = txdb_->stores_[tableid]->GetWithInsert(key);
  retry_1:
        uint64_t seq = node->seq;
        asm volatile("" ::: "memory");
        uint64_t *tmpVal = node->value;

        if(likely(tmpVal != NULL)) {
            if(unlikely(seq == 1))
                goto retry_1;
            asm volatile("" ::: "memory");
            memcpy(val,(char *)tmpVal + SI_META_LEN,len );
            asm volatile("" ::: "memory");
            if( unlikely(node->seq != seq) ) {
                goto retry_1;
            }

        } else {
            //    fprintf(stdout,"key %lu, tableid %d\n",key,tableid);
            //    assert(false);
            return 1;
        }
        return seq;
    }
#endif
    int vlen = txdb_->_schemas[tableid].vlen;
    MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
    uint64_t ret = 0;

    uint64_t *ts_vec = (uint64_t *)version;
    //ts_vec[0] = 2;

retry:
    uint64_t seq = node->seq;
    if(unlikely(seq == 0)) {
        fprintf(stdout,"tableid %d timestamp %lu, val %p\n",tableid,*ts_vec,node->value);
        //assert(false);
        return 1;
    }

    /* traverse the read linked list to find the correspond records */
    if(SI_GET_COUNTER(seq) <= ts_vec[SI_GET_SERVER(seq)] ) {
        /* simple case, read the current value */
        asm volatile("" ::: "memory");
        uint64_t *tmpVal = node->value;
        //fprintf(stdout,"Get from latest %d\n",tableid);
        if(likely(tmpVal != NULL)) {
            memcpy(val, (char *)tmpVal + SI_META_LEN,vlen);
            asm volatile("" ::: "memory");
            if(node->seq != seq || seq == 1) {
                goto retry;
            }
#if RECORD_STALE
            stale_time_buffer.add(0);
#endif
            ret = seq;
        } else {
            /* read a deleted value, currently not supported */
            assert(false);
            return 1;
        }
    } else {
        /* traverse the old reader's list */
        /* this is the simple case, and can always success  */
        //fprintf(stdout,"!!! traverse from old chain %d | seq:%lu\n", tableid,ts_vec[0]);
        char *old_val = (char *)(node->old_value);
#if RECORD_STALE
        auto  time = node->time;
#endif
        asm volatile("" ::: "memory");
        if(seq == 1 || node->seq != seq)
            goto retry;

        _SIValHeader *rh = (_SIValHeader *)old_val;
        int hop(0);
        while(old_val != NULL && SI_GET_COUNTER(rh->version) > ts_vec[SI_GET_SERVER(rh->version)]) {
            old_val = (char *)(rh->oldValue);
            rh = (_SIValHeader *)old_val;
            hop++;
        }
        if(old_val == NULL) {
            /* cannot find one */
            //ASSERT_PRINT(false,stdout,"tableid %d, ts %lu\n",tableid,*ts_vec);
#if RECORD_STALE
            auto diff = static_cast<std_time_diff_t_>(time - MemNode::init_time);
            stale_time_buffer.add(diff.count());
#endif
            return 1;
        }
        //ASSERT_PRINT(false,stdout,"tableid %d\n",tableid);
#if RECORD_STALE
        auto diff = static_cast<std_time_diff_t_>(time - rh->time);
        stale_time_buffer.add(diff.count());
#endif
        /* cpy */
        memcpy(val,(char *)old_val + SI_META_LEN,vlen);
        assert(rh->version != 0);
        ret = rh->version;
        /* in this case, we do not need to check the lock */
    }
    return ret;
}

uint64_t DBSI::get_cached(int tableid,uint64_t key,char **val) {
    for(uint i = 0;i < remoteset->elems_;++i) {
        if(remoteset->kvs_[i].tableid == tableid && remoteset->kvs_[i].key == key) {
            *val = (char *)(remoteset->kvs_[i].val);
            return remoteset->kvs_[i].seq;
        }
    }
    return 0;

}
uint64_t DBSI::get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield) {
    return _get_ro_versioned_helper(tableid,key,val,(uint64_t )ts_buffer_,yield);
}

uint64_t DBSI::get_ro_versioned(int tableid, uint64_t key, char *val, uint64_t version,yield_func_t &yield) {
    return _get_ro_versioned_helper(tableid,key,val,version,yield);
}

uint64_t
DBSI::get(int tableid, uint64_t key, char **val,int len) {

    int vlen = len + SI_META_LEN;
    vlen = vlen + 64 - vlen % 64;
    char *_addr = (char *)malloc(vlen);

    MemNode *node = NULL;
    node = txdb_->stores_[tableid]->GetWithInsert(key);

retry:
    uint64_t seq = node->seq;
    asm volatile("" ::: "memory");
#if 0

    if( SI_GET_COUNTER(seq) <= ts_buffer_[SI_GET_SERVER(seq)]) {
        //if(tableid == 1) { fprintf(stdout,"get latst version %p\n",node);}
        uint64_t *temp_val = node->value;

        if(unlikely(temp_val == NULL)){
            /* actually it is possible */
            //	fprintf(stdout,"seq 1 table %d\n",tableid);
            //	assert(false);
            return 1;
        }
        /* reads the current value */
        memcpy(_addr + SI_META_LEN,(char *)temp_val + SI_META_LEN,len);
        asm volatile("" ::: "memory");
        if(node->seq != seq || seq == 1) {
            goto retry;
        }
    } else {
        //if(tableid == 1) { fprintf(stdout,"traverse old version\n");}
        /* traverse the old reader's list */
        char *old_val = (char *)(node->old_value);
        _SIValHeader *rh = (_SIValHeader *)old_val;
        while(old_val != NULL && SI_GET_COUNTER(rh->version) > ts_buffer_[SI_GET_SERVER(rh->version)]) {
            old_val = (char *)(rh->oldValue);
            rh = (_SIValHeader *)old_val;
        }
        //	assert(old_val != NULL);
        if(unlikely(old_val == NULL)){
            //fprintf(stdout,"seq %lu,current vec %lu\n",seq,ts_buffer_[0]);
            seq = 1;
            goto READ_END;
        }
        seq = rh->version;
        memcpy(_addr + sizeof(_SIValHeader),(char *)old_val + sizeof(_SIValHeader),len);
    }
#else
    /* normal occ get */

retry_1:
    seq = node->seq;

    asm volatile("" ::: "memory");
    if(unlikely(seq == 1)) goto retry;
    uint64_t *tmpVal = node->value;

    if(unlikely(tmpVal == NULL)) {
        //assert(false);
        return 1;
    }
    //  asm volatile("" ::: "memory");
    memcpy(_addr + SI_META_LEN,(char *)tmpVal + SI_META_LEN,len);
    asm volatile("" ::: "memory");
    if( unlikely(node->seq != seq) ) {
        goto retry_1;
    }

#endif
READ_END:
    assert(seq != 0);
    WriteSet::WriteSetItem item;
    item.tableid = tableid;
    item.key = key;
    item.len = len;
    item.node = node;
    item.addr = (uint64_t *)_addr;
    item.seq = seq;
    item.ro  = true;
    *val = ( (char *)_addr + SI_META_LEN);
    assert(seq != 0);
    rwset->Add(item);
    return seq;
}


#if USE_LOGGER
inline void prepare_log(int cor_id, DBLogger* db_logger, const DBSI::WriteSet::WriteSetItem& item){
    char* val = db_logger->get_log_entry(cor_id, item.tableid, item.key, item.len);
    // printf("%p %p %d %lu\n", val, (char*)item.addr + META_LENGTH, item.len, item.key);
    memcpy(val, (char*)item.addr + SI_META_LEN, item.len);
    db_logger->close_entry(cor_id);
}
#endif
void DBSI::insert(int tableid, uint64_t key, char *val, int len) {

    int vlen = SI_META_LEN + len;
    //  vlen = vlen + 64 - vlen % 64;

    MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
    //assert(node->value == NULL && node->seq == 0);

    WriteSet::WriteSetItem item;

    item.tableid = tableid;
    item.key     = key;
    item.len     = len;
    item.node    = node;
    item.addr    = (uint64_t *)(new char[vlen]);
    memcpy( (char *)item.addr + SI_META_LEN, val,len);
    item.seq     = node->seq;
    item.ro      = false;
    rwset->Add(item);
#if USE_LOGGER
    if(db_logger_){
        // printf("insert size:%d ,key:%lu\n",item.len, item.key);
        prepare_log(cor_id_, db_logger_, item);
    }
#endif
}

void DBSI::delete_(int tableid, uint64_t key) {

    for(uint i = 0;i < rwset->elems;++i) {
        if(rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
            WriteSet::WriteSetItem &item = rwset->kvs[i];
            item.ro = false;
            // DZY:: does here miss : "delete item.addr;"" ?
            item.addr = NULL;
            item.len = 0;
#if USE_LOGGER
            if(db_logger_){
                // printf("delete_ size:%d\n",item.len);
                prepare_log(cor_id_, db_logger_, item);
            }
#endif
            return;
        }
    }

    MemNode *node = txdb_->stores_[tableid]->GetWithInsert(key);
    WriteSet::WriteSetItem item;

    item.tableid = tableid;
    item.key     = key;
    item.len     = 0;
    item.node    = node;
    item.addr    = NULL; // NUll means logical delete
    item.seq     = node->seq;
    item.ro      = false;
    rwset->Add(item);
#if USE_LOGGER
    if(db_logger_){
        // printf("delete_ size:%d\n",item.len);
        prepare_log(cor_id_, db_logger_, item);
    }
#endif
}

void DBSI::write() {
    WriteSet::WriteSetItem &item = rwset->kvs[rwset->elems - 1];
    item.ro = false;
#if USE_LOGGER
    if(db_logger_){
        // printf("write() size:%d\n",item.len);
        prepare_log(cor_id_, db_logger_, item);
    }
#endif
}

void DBSI::write(int tableid,uint64_t key,char *val,int len) {
    for (uint i = 0; i < rwset->elems; i++) {
        if (rwset->kvs[i].tableid == tableid && rwset->kvs[i].key == key) {
            WriteSet::WriteSetItem& item = rwset->kvs[i];
            item.ro = false;
#if USE_LOGGER
            if(db_logger_){
                // printf("write(...) size:%d ,key:%lu\n",item.len, item.key);
                prepare_log(cor_id_, db_logger_, item);
            }
#endif
            return;
        }
    }

    fprintf(stderr,"@%lu, local write operation not in the readset! tableid %d key %lu, not supported \n",
            thread_id,tableid,key);
    exit(-1);
}

void DBSI::ThreadLocalInit() {

    if (false == localinit) {
        rwset = new WriteSet();
        remoteset = new RemoteSet(rpc_,cor_id_,thread_id);
        ts_buffer_ = (uint64_t *)(Rmalloc(ts_manager->ts_size_));
        assert(ts_buffer_ != NULL);
        localinit = true;
    }
}

void DBSI::local_ro_begin() {
    ThreadLocalInit();
    //#ifndef EM_OCC
    ts_manager->get_start_ts((char *)ts_buffer_);
    //fprintf(stdout,"check fetched ts \n");
    //ts_manager->print_ts(ts_buffer_);
    //#endif
}

void DBSI::_begin(DBLogger *db_logger,TXProfile *p) {
    // init rw sets
#if 1
    ThreadLocalInit();
    rwset->Reset();
    abort_ = false;

    // get timestamp
#ifndef EM_OCC
    int ts_size = ts_manager->ts_size_;
    ts_manager->get_start_ts((char *)ts_buffer_);
    remoteset->clear(ts_size);
#else
    remoteset->clear();
#endif

#endif
#if USE_LOGGER
    db_logger_ = db_logger;
    if(db_logger_)db_logger->log_begin(cor_id_, 1);
#endif
}

bool
DBSI::end(yield_func_t &yield) {

    if(abort_) {
        assert(false);
        return false;
    }
    //remoteset->update_write_buf();
    //return true;
    uint64_t commit_ts,encoded_commit_ts;
    rwset->SetTX(this);


    /* lock remote sets */
    if(!remoteset->lock_remote(yield)) {
        goto ABORT;
    }

    if(unlikely(!rwset->LockAllSet(timestamp) ) )
        goto ABORT;

    /* get commit ts */
    commit_ts = get_commit_ts(yield,encoded_commit_ts);

#if USE_LOGGER
    if(db_logger_){
        db_logger_->log_backups(cor_id_, encoded_commit_ts);
        worker->indirect_must_yield(yield);
        db_logger_->log_end(cor_id_);
    }
#endif
    rwset->CommitLocalWrite(encoded_commit_ts);
    remoteset->max_time_ = encoded_commit_ts;
    remoteset->commit_remote(yield);
#ifndef EM_OCC
    this->commit_ts(commit_ts,yield); // commit the timestamp to the remote oracle
#endif
#if ONE_CLOCK == 1
    this->commit_ts(commit_ts,yield); // commit the timestamp to the remote oracle
#endif
    return true;
ABORT:
    remoteset->release_remote(yield);
    rwset->ReleaseAllSet();
#if USE_LOGGER
    if(db_logger_){
        db_logger_->log_abort(cor_id_);
    }
#endif
    return false;
}

uint64_t DBSI::get_commit_ts(yield_func_t &yield,uint64_t &encoded_commit_ts) {
    uint64_t commit_ts;
#if ONE_CLOCK == 0
#ifdef EM_OCC
    commit_ts = 12;encoded_commit_ts = commit_ts;
#else
    commit_ts = ts_manager->get_commit_ts();
#if LARGE_VEC == 1
    encoded_commit_ts = SI_ENCODE_TS(current_partition * nthreads + thread_id,
                                     commit_ts);
#else
    encoded_commit_ts = SI_ENCODE_TS(current_partition,commit_ts);
#endif
#endif

#else

    int master_id = ts_manager->master_id_;
    char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();
    *((uint64_t *)req_buf) = commit_ts;
    rpc_->prepare_multi_req((char *)(&commit_ts),1,cor_id_);
    rpc_->append_req(req_buf,RPC_TS_ACQUIRE,sizeof(uint64_t),cor_id_,RRpc::REQ,
                     master_id /* ,nthreads + nclients + 1 */);
    worker->indirect_yield(yield);
    encoded_commit_ts = SI_ENCODE_TS((uint64_t)0,commit_ts);
#endif
    return commit_ts;
}

void DBSI::commit_ts(uint64_t ts,yield_func_t &yield) {
#if TS_USE_MSG == 1
    char *req_buf = rpc_->get_fly_buf(cor_id_);
    TSManager::UpdateArg *input = (TSManager::UpdateArg *)req_buf;
    input->thread_id = thread_id;
    input->counter   = ts;
    int master_id = ts_manager->master_id_;
    //rpc_handler_->send_reqs(RPC_COMMIT + 1,sizeof(TSManager::UpdateArg),&master_id,1,cor_id_);
    //rpc_handler_->append_req(req_buf,RPC_COMMIT + 1,sizeof(TSManager::UpdateArg),
    //master_id,nthreads,cor_id_);
    rpc_->append_req(req_buf,RPC_COMMIT + 1,sizeof(TSManager::UpdateArg),
                     cor_id_,RRpc::REQ,master_id);
    return;
#endif

    // set the meta data for posting commit timestamp
    *ts_buffer_ = ts;
    Qp *qp = qp_vec_[ts_manager->master_id_];
#define OPT 1
#if LARGE_VEC == 1
    uint64_t offset = current_partition * nthreads + thread_id + ts_manager->ts_addr_;
#if !OPT
    auto send_flag = 0;
    auto ret = qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)ts_buffer_,sizeof(uint64_t),
                                offset * sizeof(uint64_t),IBV_SEND_SIGNALED | IBV_SEND_INLINE);
    assert(ret == Qp::IO_SUCC);
    ret = qp->poll_completion(); // FIXME!! no error detection now
    assert(ret == Qp::IO_SUCC);
#else                   // a fast version of posting RDMA writes
    int send_flag = IBV_SEND_INLINE;
    if(qp->first_send()) {
        send_flag |= IBV_SEND_SIGNALED;
    }
    if(qp->need_poll())
        qp->poll_completion();
    auto ret = qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)ts_buffer_,sizeof(uint64_t),
                                offset * sizeof(uint64_t),send_flag);
    assert(ret == Qp::IO_SUCC);
#endif
#else // small vector timestamp case
    // update the commit timestamp, using dedicated QP
    //while(ts_manager_->last_ts_ != ts - 1) {
    //worker->yield_next(yield);
    //asm volatile("" ::: "memory");
    //}
    // this is the distributed SI case
    // an RDMA write is needed
    {
        // must be **synchronously** posted, otherwise a later one may be overwritten by other
        // threads
#if !OPT
        auto send_flag = 0;
        auto ret = qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)ts_buffer_,sizeof(uint64_t),
                                    current_partition * sizeof(uint64_t),IBV_SEND_SIGNALED);
        assert(ret == Qp::IO_SUCC);

        qp->poll_completion(); // FIXME!! no error detection now
#else
        int send_flag = IBV_SEND_INLINE;
        if(qp->first_send()) {
            send_flag |= IBV_SEND_SIGNALED;
        }
        if(qp->need_poll())
            qp->poll_completion();
        auto ret = qp->rc_post_send(IBV_WR_RDMA_WRITE,(char *)ts_buffer_,sizeof(uint64_t),
                                    current_partition * sizeof(uint64_t),send_flag);
        assert(ret == Qp::IO_SUCC);
#endif
    }

    asm volatile("" ::: "memory");
    TSManager::last_ts += 1;
#endif                // end post per_mac timestamp
} // end commit ts function

void DBSI::abort() {
#if USE_LOGGER
    if(db_logger_){
        db_logger_->log_abort(cor_id_);
    }
#endif
}

SIIterator::SIIterator (DBSI *tx,int tableid,bool sec) {
    tx_ = tx;
    if(sec) {
        iter_ = (tx_->txdb_->_indexs[tableid])->GetIterator();
    } else {
        iter_ = (tx_->txdb_->stores_[tableid])->GetIterator();
    }
    cur_ = NULL;
    prev_link = NULL;
}

bool SIIterator::Valid() {
    /* maybe we need a snapshot one ?*/
    return cur_ != NULL && cur_->seq != 0 ;
}

uint64_t SIIterator::Key()
{
    return iter_->Key();
}

char *SIIterator::Node() {
    return (char *)cur_;
}

char *SIIterator::Value() {
    return (char *)val_ ;
}

void SIIterator::Next() {

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

void SIIterator::Prev() {

    bool b = iter_->Prev();
    if(!b) {
        //  tx_->abort = true;
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

void SIIterator::Seek(uint64_t key) {

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
                return;
            }
        }
        iter_->Next();
        cur_ = iter_->CurNode();

    }

    cur_ = NULL;
}

void SIIterator::SeekToFirst() {
    /* TODO ,not implemented. seems not needed */
}

void SIIterator::SeekToLast() {
    /* TODO ,not implemented */
}

int  DBSI::add_to_remote_set(int tableid,uint64_t key,int pid) {
    return remoteset->add(REQ_READ,pid,tableid,key);
}

void DBSI::remote_write(int tableid,uint64_t key,char *val,int len) {
    // TODO!!
}

void DBSI::do_remote_reads(yield_func_t &yield) {
    // not supported any more
    assert(false);
    remoteset->do_reads(yield);
}

int
DBSI::do_remote_reads() {
    // add timestamp to rpc's meta data
#ifndef EM_OCC
    char *remote_ts = remoteset->get_meta_ptr();
    memcpy(remote_ts,ts_buffer_,ts_manager->ts_size_);
#endif
    return remoteset->do_reads();
}

void DBSI::get_remote_results(int num_results) {
    remoteset->get_results(num_results);
    //remoteset->clear_for_reads();
}

void DBSI::insert_index(int tableid, uint64_t key, char *val) {

    MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
    WriteSet::WriteSetItem item;

    item.tableid = tableid;
    item.key     = key;
    item.node    = node;
    item.addr    = (uint64_t *)val;
    item.seq     = node->seq;
    item.ro      = false;
    rwset->Add(item);
}

void DBSI::delete_index(int tableid,uint64_t key) {
    MemNode *node = txdb_->_indexs[tableid]->GetWithInsert(key);
    if(unlikely(node->value == NULL)) {
        //fprintf(stdout,"index %d null, check symbol %s using %p\n",
        //	      tableid,(char *)( &(((uint64_t *)key)[2])),node);
        //      assert(false);
        // possible found by the workload
        return;
    }
    WriteSet::WriteSetItem item;
    item.tableid = tableid;
    item.key     = key;
    item.node    = node;
    item.addr    = NULL;
    item.seq     = node->seq;
    item.ro      = false;
    rwset->Add(item);
}

void DBSI::delete_by_node(int tableid,char *node) {
    for(uint i = 0;i < rwset->elems;++i) {
        if((char *)(rwset->kvs[i].node) == node) {
            WriteSet::WriteSetItem &item = rwset->kvs[i];
            item.ro = false;
            delete item.addr;
            item.addr = NULL;
            item.len = 0;
#if USE_LOGGER
            if(db_logger_){
                // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
                prepare_log(cor_id_, db_logger_, item);
            }
#endif
            return;
        }

    }
    WriteSet::WriteSetItem item;

    item.tableid = tableid;
    item.key     = 0;
    item.len     = 0;
    item.node    = (MemNode *)node;
    item.addr    = NULL; // NUll means logical delete
    item.seq     = item.node->seq;
    item.ro      = false;
    rwset->Add(item);
#if USE_LOGGER
    if(db_logger_){
        // printf("delete_by_node size:%d ,key:%lu\n",item.len, item.key);
        prepare_log(cor_id_, db_logger_, item);
    }
#endif
}

int DBSI::add_to_remote_set(int tableid,uint64_t *key,int klen,int pid) {
    return remoteset->add(REQ_READ,pid,tableid,key,klen);
}

int DBSI::remote_read_idx(int tableid,uint64_t *key,int klen,int pid) {
    return remoteset->add(REQ_READ_IDX,pid,tableid,key,klen);
}

int DBSI::remote_insert(int tableid,uint64_t *key, int klen,int pid) {
    return remoteset->add(REQ_INSERT,pid,tableid,key,klen);
}

int DBSI::remote_insert_idx(int tableid,uint64_t *key, int klen,int pid) {
    return remoteset->add(REQ_INSERT_IDX,pid,tableid,key,klen);
}

void DBSI::remote_write(int idx,char *val,int len) {
    assert(remoteset->cor_id_ == cor_id_);
#if USE_LOGGER
    if(db_logger_){
        // printf("remote_write size: %d\n", len);
        RemoteSet::RemoteSetItem& item = remoteset->kvs_[idx];
        char* logger_val = db_logger_->get_log_entry(cor_id_, item.tableid, item.key, len, item.pid);
        memcpy(logger_val, val, len);
        db_logger_->close_entry(cor_id_);
    }
#endif
    remoteset->promote_to_write(idx,val,len);
}

uint64_t DBSI::get_cached(int idx,char **val) {
    if(unlikely(remoteset->kvs_[idx].seq == 0))
        return 1;
    *val = (char *)(remoteset->kvs_[idx].val);
    return remoteset->kvs_[idx].seq;
}
} // namespace db
} // namespace nocc
