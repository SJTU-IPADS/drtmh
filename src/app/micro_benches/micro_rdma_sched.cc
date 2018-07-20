#include "bench_micro.h"
#include "tx_config.h"

namespace nocc {

  namespace oltp {
    
    namespace micro {

      txn_result_t MicroWorker::micro_rdma_sched(yield_func_t &yield) {
        int target_id = current_partition;
        int num_nodes = cm_->get_num_nodes();
        int target_nums = num_nodes - 1;
        for (int i = 0; i < target_nums; i++) {
          target_id = (target_id + 1) % num_nodes;
          Qp* qp = qps_[target_id]; 
          qp->rc_post_send(IBV_WR_RDMA_READ,rdma_buf_,512,(uint64_t)1024*1024*1024*12,IBV_SEND_SIGNALED);
          // rdma_sched_->add_pending(cor_id_, qp);
        }
        indirect_must_yield(yield);
        
        
        return txn_result_t(true,0);
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
