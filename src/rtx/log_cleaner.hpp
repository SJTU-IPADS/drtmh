#pragma once

#include "core/rrpc.h"
#include "log_store_manager.hpp"

namespace nocc {

namespace rtx {

using namespace oltp;

// The cleaner leverages log_store_manager to access backup stores
class LogCleaner : public LogStoreManager {
 public:
  explicit LogCleaner(int num) : LogStoreManager(num) {

  }

  // the register_callback is used to register *clean_log*
  virtual void register_callback(int id,RRpc *rpc) = 0;

  virtual void clean_log(int nid,int cid,char *log,void *) = 0;
};

}; // namespace rtx
}; // namespace nocc
