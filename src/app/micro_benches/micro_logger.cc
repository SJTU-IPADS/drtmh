#include "bench_micro.h"

namespace nocc {

  namespace oltp {

    namespace micro {

      txn_result_t MicroWorker::micro_logger_write(yield_func_t &yield) {
        return txn_result_t(true,0);
      }

      txn_result_t MicroWorker::micro_logger_func(yield_func_t &yield) {
        return txn_result_t(true,0);
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
