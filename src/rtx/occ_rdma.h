#pragma once

#include "tx_config.h"

#include "occ.h"
#include "core/logging.h"

#include "checker.hpp"

namespace nocc {

namespace rtx {

/**
 * Extend baseline OCC with one-sided RDMA support for execution, validation and commit.
 */
class OCCR : public OCC {
 public:
  OCCR(oltp::RWorker *worker,MemDB *db,RRpc *rpc_handler,int nid,int tid,int cid,int response_node,
          RdmaCtrl *cm,RScheduler* rdma_sched,int ms) :
      OCC(worker,db,rpc_handler,nid,tid,cid,response_node,
             cm,rdma_sched,ms)
  {
    if(worker_id_ == 0 && cor_id_ == 0) {
      LOG(3) << "Use one-sided for read.";
    }

    // register normal RPC handlers
    register_default_rpc_handlers();

    // overwrites with default RPC handler
    /**
     * These RPC handlers does not operate on value/meta data in the index.
     * This can be slightly slower than default RPC handler for *LOCK* and *validate*.
     */
    //ROCC_BIND_STUB(rpc_,&OCCR::lock_rpc_handler2,this,RTX_LOCK_RPC_ID);
    //ROCC_BIND_STUB(rpc_,&OCCR::validate_rpc_handler2,this,RTX_VAL_RPC_ID);
    //ROCC_BIND_STUB(rpc_,&OCCR::commit_rpc_handler2,this,RTX_COMMIT_RPC_ID);
  }

  /**
   * Using RDMA one-side primitive to implement various TX operations
   */
  bool lock_writes_w_rdma(yield_func_t &yield);
  void write_back_w_rdma(yield_func_t &yield);
  bool validate_reads_w_rdma(yield_func_t &yield);
  void release_writes_w_rdma(yield_func_t &yield);

  int pending_remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {

    ASSERT(RDMA_CACHE) << "Current RTX only supports pending remote read for value in cache.";

    char *data_ptr = (char *)Rmalloc(sizeof(MemNode) + len);
    assert(data_ptr != NULL);

    auto off = pending_rdma_read_val(pid,tableid,key,len,data_ptr,yield,sizeof(RdmaValHeader));
    data_ptr += sizeof(RdmaValHeader);

    read_set_.emplace_back(tableid,key,(MemNode *)off,data_ptr,
                           0,
                           len,pid);
    return read_set_.size() - 1;
  }

  int remote_read(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {

    char *data_ptr = (char *)Rmalloc(sizeof(MemNode) + len);
    ASSERT(data_ptr != NULL);

    uint64_t off = 0;
#if INLINE_OVERWRITE
    off = rdma_lookup_op(pid,tableid,key,data_ptr,yield);
    MemNode *node = (MemNode *)data_ptr;
    auto seq = node->seq;
    data_ptr = data_ptr + sizeof(MemNode);
#else
    off = rdma_read_val(pid,tableid,key,len,data_ptr,yield,sizeof(RdmaValHeader));
    RdmaValHeader *header = (RdmaValHeader *)data_ptr;
    auto seq = header->seq;
    data_ptr = data_ptr + sizeof(RdmaValHeader);
#endif
    ASSERT(off != 0) << "RDMA remote read key error: tab " << tableid << " key " << key;

    read_set_.emplace_back(tableid,key,(MemNode *)off,data_ptr,
                           seq,
                           len,pid);
    return read_set_.size() - 1;
  }

  bool commit(yield_func_t &yield) {

#if TX_ONLY_EXE
    return dummy_commit();
#endif

#if 0 //USE_RDMA_COMMIT
    if(!lock_writes_w_rdma(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#else
    if(!lock_writes(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#endif

#if CHECKS
    RdmaChecker::check_lock_content(this,yield);
#endif

    asm volatile("" ::: "memory");
#if 1 //USE_RDMA_COMMIT
    if(!validate_reads_w_rdma(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#else
    if(!validate_reads(yield)) {
#if !NO_ABORT
      goto ABORT;
#endif
    }
#endif

#if 1
    asm volatile("" ::: "memory");
    prepare_write_contents();
    log_remote(yield); // log remote using *logger_*
#if CHECKS
    RdmaChecker::check_log_content(this,yield);
#endif

    // clear the mac_set, used for the next time
    write_batch_helper_.clear();

    asm volatile("" ::: "memory");
#endif
#if 0
    write_back_w_rdma(yield);
#else
    /**
     * Fixme! write back w RPC now can only work with *lock_w_rpc*.
     * This is because lock_w_rpc helps fill the mac_set used in write_back.
     */
    write_back_oneshot(yield);
#endif

#if CHECKS
    RdmaChecker::check_backup_content(this,yield);
#endif
    gc_readset();
    gc_writeset();
    return true;
 ABORT:
#if 0 //USE_RDMA_COMMIT
    release_writes_w_rdma(yield);
#else
    release_writes(yield);
#endif

    gc_readset();
    gc_writeset();
    // clear the mac_set, used for the next time
    write_batch_helper_.clear();
    return false;
  }

  /**
   * GC the read/write set is a little complex using RDMA.
   * Since some pointers are allocated from the RDMA heap, not from local heap.
   */
  void gc_helper(std::vector<ReadSetItem> &set) {
    for(auto it = set.begin();it != set.end();++it) {
      if(it->pid != node_id_) {
#if INLINE_OVERWRITE
        Rfree((*it).data_ptr - sizeof(MemNode));
#else
        Rfree((*it).data_ptr - sizeof(RdmaValHeader));
#endif
      }
      else
        free((*it).data_ptr);

    }
  }

  // overwrite GC functions, to use Rfree
  void gc_readset() {
    gc_helper(read_set_);
  }

  void gc_writeset() {
    gc_helper(write_set_);
  }

  bool dummy_commit() {
    // clean remaining resources
    gc_readset();
    gc_writeset();
    return true;
  }

  /**
   * A specific lock handler, use the meta data encoded in value
   * This is because we only cache one address, so it's not so easy to
   * to encode meta data in the index (so that we need to cache the index).
   */
  void lock_rpc_handler2(int id,int cid,char *msg,void *arg);
  void commit_rpc_handler2(int id,int cid,char *msg,void *arg);
  void release_rpc_handler2(int id,int cid,char *msg,void *arg);
  void validate_rpc_handler2(int id,int cid,char *msg,void *arg);
};

} // namespace rtx
} // namespace nocc
