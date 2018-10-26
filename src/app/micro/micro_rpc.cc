#include "tx_config.h"
#include "micro_worker.h"

extern size_t distributed_ratio; // used for some app defined parameters
extern size_t total_partition;

namespace nocc {
namespace oltp {

extern __thread util::fast_random   *random_generator;

namespace micro {

struct Req {
  uint8_t size = 0;
};

// RPC IDs
enum {
  REQ_ID = 0
};

extern uint64_t working_space;
txn_result_t MicroWorker::micro_rpc(yield_func_t &yield) {

  static const int num_nodes = total_partition;

  auto size = distributed_ratio;
  assert(size > 0 && size <= MAX_MSG_SIZE);

  int window_size = 10;
  ASSERT(window_size < 64) << "window size shall be smaller than 64";
  static __thread char *req_buf   = rpc_->get_static_buf(4096);
  static __thread char *reply_buf = (char *)malloc(1024);

  char *req_ptr = req_buf;
  ASSERT(size <= 4096);

#if 1
#if !PA
  rpc_->prepare_multi_req(reply_buf, window_size,cor_id_);
#endif

  for (uint i = 0; i < window_size; ++i) {

    int pid = random_generator[cor_id_].next() % num_nodes;

    // prepare an RPC header
    Req *req = (Req *)(req_ptr);
    req->size = size;
#if 1
    rpc_->append_pending_req((char *)req,REQ_ID,sizeof(Req),cor_id_,RRpc::REQ,pid);
#else
    rpc_->append_req((char *)req_buf,REQ_ID,sizeof(Req),cor_id_,RRpc::REQ,pid);
#endif
  }
  rpc_->flush_pending();
#endif

#if !PA
  indirect_yield(yield);
#endif
  ntxn_commits_ += (window_size - 1);

  return txn_result_t(true,1);
}

/**
 * RPC handlers
 */

void MicroWorker::register_callbacks() {
  ROCC_BIND_STUB(rpc_,&MicroWorker::nop_rpc_handler,this,REQ_ID);
}

void MicroWorker::nop_rpc_handler(int id,int cid,char *msg, void *arg) {
  char *reply_msg = rpc_->get_reply_buf();
  Req *req = (Req *)msg;
  ASSERT(req->size <= ::rdmaio::UDRecvManager::MAX_PACKET_SIZE)
      << "req size "<< (int)(req->size) << " " << ::rdmaio::UDRecvManager::MAX_PACKET_SIZE;
  rpc_->send_reply(reply_msg,req->size,id,worker_id_,cid); // a dummy notification
}

} // end micro
}
}
