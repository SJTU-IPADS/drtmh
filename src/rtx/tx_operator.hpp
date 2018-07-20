#pragma once

#include "memstore/memdb.h"

#include "core/rrpc.h"
#include "core/common.h"

#include "rdmaio.h"

namespace nocc {

namespace rtx {

// This class implements multiple TX operators that is required for common concurrency control,
// such as 2-phase locking, optimistic concurrency control and snapshot isolation
using namespace nocc::oltp;
using namespace rdmaio;

struct   BatchOpCtrlBlock;

typedef  uint64_t short_key_t;
typedef  uint8_t  tableid_t;
typedef  uint8_t  partition_id_t;

class TXOpBase {
 public:
  TXOpBase() { }

  // allow op implementation based on RPC
  TXOpBase(MemDB *db,RRpc *rpc_handler,int nid):
      db_(db),rpc_(rpc_handler),node_id_(nid),worker_id_(rpc_handler->worker_id_) {

  }

  // allow op implementation based on RDMA one-sided operations
  TXOpBase(MemDB *db,RRpc *rpc_handler,RdmaCtrl *cm, RDMA_sched* rdma_sched,
           int nid, // my node id
           int tid, // worker thread's id
           int ms)  // total macs in the cluster setting
      :db_(db),cm_(cm),scheduler_(rdma_sched),node_id_(nid),worker_id_(tid),rpc_(rpc_handler) {
    // fetch QPs
    for(uint i = 0;i < ms;++i) {
      auto qp = cm->get_rc_qp(tid,i,0);
      assert(qp != NULL);
      qp_vec_.push_back(qp);
    }
  }

  // get ops
  MemNode *local_get_op(int tableid,uint64_t key,char *val,int len,uint64_t &seq,int meta_len = 0);

  MemNode *local_insert_op(int tableid,uint64_t key,uint64_t &seq);

  // NULL: lock failed
  MemNode *local_try_lock_op(int tableid,uint64_t key,int lock_content);
  bool     local_try_lock_op(MemNode *node,int lock_content);

  // whether release is succesfull, according to the lock_content
  bool     local_try_release_op(int tableid,uint64_t key,int lock_content);
  bool     local_try_release_op(MemNode *node,int lock_content);

  bool     local_validate_op(int tableid,uint64_t key,uint64_t seq);
  bool     local_validate_op(MemNode *node,uint64_t seq);

  void     inplace_write_op(int tableid,uint64_t key,char *val,int len);
  void     inplace_write_op(MemNode *node,char *val,int len);

  // basically its only a wrapper to send a get request with Argument REQ
  template <typename REQ,typename... _Args>
  uint64_t rpc_op(int cor_id,int rpc_id,int pid,char *req_buf,char *res_buf,_Args&& ... args);

  // lookup the MemNode, stored in val (MemNode), return is the offset
  uint64_t     rdma_lookup_op(int pid,int tableid,uint64_t key,char *val,yield_func_t &yield,int meta_len = 0);

  /*
   * Batch operations
   * A control block, shall be passed to indicate the whole control operation
   */
  void     start_batch_rpc_op(BatchOpCtrlBlock &ctrl);

  template <typename REQ,typename... _Args> // batch req
  void     add_batch_entry(BatchOpCtrlBlock &ctrl,int pid,_Args&& ... args);

  template <typename REQ,typename... _Args> // batch req
  void     add_batch_entry_wo_mac(BatchOpCtrlBlock &ctrl,int pid,_Args&& ... args);

  int      send_batch_rpc_op(BatchOpCtrlBlock &ctrl,int cor_id,int rpc_id,bool pa = 0);
  template <typename REPLY> // reply type
  REPLY    *get_batch_res(BatchOpCtrlBlock &ctrl,int idx);  // return the results to the pointer of result buffer

 protected:
  MemDB *db_  = NULL;
  RRpc  *rpc_ = NULL;
  RdmaCtrl *cm_ = NULL;
  RDMA_sched *scheduler_ = NULL;

  std::vector<Qp *> qp_vec_;

  int node_id_;
  int worker_id_;

  DISABLE_COPY_AND_ASSIGN(TXOpBase);
}; // TX ops

// helper macros for iterating message in rpc handler
#define RTX_ITER_ITEM(msg,size)                                 \
  int i;int num;char *ttptr;                                    \
  for(i = 0,ttptr = (msg) + sizeof(RTXRequestHeader),           \
    num = ((RTXRequestHeader *)msg)->num;                       \
      i < num; i++, ttptr += size)

}  // namespace rtx

}; // namespace nocc

// Real implementations
#include "tx_get_op_impl.hpp"
#include "tx_batch_op_impl.hpp"
#include "tx_rdma_op_impl.hpp"
