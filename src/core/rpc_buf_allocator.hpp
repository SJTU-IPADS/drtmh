#pragma once

#include <vector>

#include "ralloc/ralloc.h" // RDMA malloc

namespace nocc {

/**
 * The buffer used by rpc should be carefully allocated (from registered memory).
 * This class help this
 */
class RPCBufAllocator {
 public:
  RPCBufAllocator(int req_buf_num,int reply_buf_num,int req_msg_size,int reply_msg_size,int coroutines) {

    assert(req_buf_num > 0 && reply_buf_num > 0);

    RThreadLocalInit();

    for(uint i = 0;i < coroutines + 1;++i) {
      req_buf_slots_.push_back(0);
      req_buf_pool_.push_back(std::vector<char *> ());

      for(uint j = 0;j < req_buf_num;++j) {
        char *buf = (char *)Rmalloc(req_msg_size);
        req_buf_pool_[i].push_back(buf);
      }
    }

    for(uint i = 0;i < reply_buf_num;++i) {
      char *buf = (char *)Rmalloc(reply_msg_size);
      reply_buf_pool_.push_back(buf);
    }
  }

  // Get a reply buffer for the RPC handler
  inline char *get_reply_buf() {
    auto res = reply_buf_pool_[(reply_buf_slot_++) % reply_buf_pool_.size()];
    return res;
  }

  // Get a buffer for sending RPC request.
  // This buffer can be re-used.
  inline char *get_fly_buf(int cid) {
    char *res = req_buf_pool_[cid][(req_buf_slots_[cid]++) % req_buf_pool_[cid].size()];
    return res;
  }

 private:
  // buffer pools
  std::vector<char *> reply_buf_pool_;
  uint16_t reply_buf_slot_ = 0;

  std::vector<std::vector<char *> >req_buf_pool_;
  std::vector<uint8_t> req_buf_slots_;
};

};
