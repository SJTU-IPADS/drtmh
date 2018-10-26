#ifndef APP_MICRO
#define APP_MICRO

#include "tx_config.h"
#include "framework/bench_worker.h"

namespace nocc {
namespace oltp {

namespace micro {

enum MICRO_TYPE {
  RPC        = 0,
  RDMA_READ  = 1,
  RDMA_WRITE = 2,
  RDMA_CAS   = 3
};

void MicroTest(int argc,char **argv); // main hook function

class MicroWorker : public BenchWorker {
 public:
  MicroWorker(unsigned int worker_id,unsigned long seed,int micro_type,MemDB *store,
              uint64_t total_ops, spin_barrier *a,spin_barrier *b,BenchRunner *c);

  void register_callbacks();
  void thread_local_init();

  workload_desc_vec_t get_workload() const ;

  void workload_report() {
	REPORT(post);
  }

  /**
   * real worker function body
   */
  txn_result_t micro_rpc(yield_func_t &yield);
  txn_result_t micro_rdma_read(yield_func_t &yield);
  txn_result_t micro_rdma_write(yield_func_t &yield);
  txn_result_t micro_rdma_atomic(yield_func_t &yield);
  //

  /**
   * RPC handlers
   */
  void nop_rpc_handler(int id,int cid,char *msg,void *arg);
  //

 private:
  static  workload_desc_vec_t _get_workload();

  static txn_result_t MicroRpc(BenchWorker *w,yield_func_t &yield) {
    return static_cast<MicroWorker *>(w)->micro_rpc(yield);
  }

  static txn_result_t MicroRdmaRead(BenchWorker *w,yield_func_t &yield) {
    return static_cast<MicroWorker *>(w)->micro_rdma_read(yield);
  }

  static txn_result_t MicroRdmaWrite(BenchWorker *w,yield_func_t &yield) {
    return static_cast<MicroWorker *>(w)->micro_rdma_write(yield);
  }

  static txn_result_t MicroRdmaAtomic(BenchWorker *w,yield_func_t &yield) {
    return static_cast<MicroWorker *>(w)->micro_rdma_atomic(yield);
  }

  std::vector<RCQP *> qp_vec_;

  // some performance counters
  LAT_VARS(post);
}; // end class micro rpc

}

} //
}

#endif
