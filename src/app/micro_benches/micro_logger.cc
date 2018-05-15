#include "bench_micro.h"

namespace nocc {

  namespace oltp {

    namespace micro {

      txn_result_t MicroWorker::micro_logger_write(yield_func_t &yield) {
        return txn_result_t(true,0);
      }

      txn_result_t MicroWorker::micro_logger_func(yield_func_t &yield) {

        char* ptr;
        db_logger_->log_begin(cor_id_);

        db_logger_->get_log_entry(cor_id_,7,0x00001010,8);
        db_logger_->close_entry(cor_id_,4);

        ptr = db_logger_->get_log_entry(cor_id_,4,0x03201010,8,1);
        *((uint64_t*)ptr) = current_partition;
        db_logger_->close_entry(cor_id_,8);

        db_logger_->get_log_entry(cor_id_,34,0x03203410,8,0);
        db_logger_->close_entry(cor_id_,16);

        db_logger_->log_backups(cor_id_);
        indirect_must_yield(yield);
        db_logger_->log_end(cor_id_);

        return txn_result_t(true,0);
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
