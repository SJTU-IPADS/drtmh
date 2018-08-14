#pragma once

#include <map>
#include "core/rrpc.h"

#include "tx_config.h"

#include "tx_operator.hpp"
#include "log_mem_manager.hpp"
#include "default_log_cleaner_impl.hpp"

namespace nocc {

namespace rtx {

class RdmaChecker;
class OCC;

// Abstract logger class
class Logger {
 public:
  /*
   * rpc: rpc_handler
   * expected_store_num: backup stores at this node
   * local_p:            log memory buffer pointer
   * ms:                 total mac log expected
   * ts:                 total thread at each mac
   * size:               total log area of the mac
   * entry_size:         the max size of each log
   */
  Logger(RRpc *rpc,int ack_rpc_id,uint64_t base_off,
         int expected_store_num,char *local_p,int ms,int ts,int size,int entry_size = RTX_LOG_ENTRY_SIZE):
      mem_(local_p,ms,ts,size,entry_size,base_off),
      cleaner_(expected_store_num,rpc),
      rpc_handler_(rpc),
      ack_rpc_id_(ack_rpc_id),
      reply_buf_((char *)malloc(ms * sizeof(uint64_t)))

  {
    cleaner_.register_callback(ack_rpc_id_,rpc_handler_);
  }

  ~Logger() {
    free(reply_buf_);
  }

  // log remote remote server defined in mac_set
  virtual void log_remote(BatchOpCtrlBlock &ctrl,int cor_id) = 0;

  virtual void check_remote(BatchOpCtrlBlock &ctrl,int cor_id,yield_func_t &yield) {
  }

  // ack log at remote servers
  // a helper function to broadcast this message to others
  virtual void log_ack(BatchOpCtrlBlock &ctrl,int cor_id) {
    ctrl.send_batch_op(rpc_handler_,cor_id,ack_rpc_id_,PA);
    // a yield call is necessary after this
  }

  inline void add_backup_store(int id,MemDB *backup_store) {
    cleaner_.add_backup_store(id,backup_store);
  }

 protected:
  LogMemManager      mem_;
  DefaultLogCleaner  cleaner_;
  RRpc *rpc_handler_ = NULL;
  char *reply_buf_   = NULL;

 private:
  const int ack_rpc_id_;

  friend RdmaChecker;
  friend OCC;

  DISABLE_COPY_AND_ASSIGN(Logger);
}; // logger

}; // namespace rtx

};   // namespace nocc

// RTX provides two logger implementation
#include "rdma_logger_impl.hpp"
#include "rpc_logger_impl.hpp"
