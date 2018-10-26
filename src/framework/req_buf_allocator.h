#ifndef NOCC_DB_MEM_ALLOCATOR
#define NOCC_DB_MEM_ALLOCATOR

#include "all.h"
#include "config.h"
#include "./config.h"
#include "ralloc/ralloc.h"
#include "core/rrpc.h"

#include <stdint.h>
#include <queue>

namespace nocc {

  namespace oltp {

    // RPC memory allocator
    class RPCMemAllocator {
    public:

      RPCMemAllocator() {
        RThreadLocalInit();
        for(int i = 0;i < MAX_INFLIGHT_REQS;++i) {
#if 1
          // FIXME: this is legacy code, so we hard coded this. shall be removed later
          buf_pools_[i] = (char *)Rmalloc(MAX_MSG_SIZE) + sizeof(uint64_t) + sizeof(uint64_t);
          // check for alignment
          assert( ((uint64_t)(buf_pools_[i])) % 8 == 0);
          if(buf_pools_[i] != NULL)
            memset(buf_pools_[i],0,MAX_MSG_SIZE);
          else
            assert(false);
#endif
        }
        current_buf_slot_ = 0;
      }

      inline char * operator[] (int id) const{
        return buf_pools_[id];
      }

      inline void   rollback_buf() {
        current_buf_slot_ = current_buf_slot_ - 1;
      }

      inline char * get_req_buf() {

        uint16_t buf_idx = (current_buf_slot_++) % MAX_INFLIGHT_REQS;
        assert(buf_idx >= 0 && buf_idx < MAX_INFLIGHT_REQS);
        // fetch the buf
        char *res = buf_pools_[buf_idx];
        return res;
      }

      inline char *post_buf(int num = 1) {
        current_freed_slot_ += num;
      }

    private:
      char *buf_pools_[MAX_INFLIGHT_REQS];
      uint64_t current_buf_slot_;
      uint64_t current_freed_slot_;
    };
  } // namespace db
};  // namespace nocc

#endif
