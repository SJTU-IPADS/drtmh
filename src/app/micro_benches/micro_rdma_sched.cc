#include "bench_micro.h"
#include "tx_config.h"

namespace nocc {

  namespace oltp {
    
    namespace micro {

      txn_result_t MicroWorker::micro_rdma_sched(yield_func_t &yield) {

        
        return txn_result_t(true,0);
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
