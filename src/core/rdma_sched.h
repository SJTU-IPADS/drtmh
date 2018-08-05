#ifndef NOCC_RDMA_SCHED
#define NOCC_RDMA_SCHED

#include <deque>

#include "rdmaio.h"
#include "util/util.h"

namespace nocc {

namespace oltp {

class RScheduler {
 public:
  RScheduler();
  ~RScheduler();

  static const int  COR_ID_BIT = 8;
  static const uint  COR_ID_MASK = ::nocc::util::BitMask<uint>(COR_ID_BIT);

  // add pending qp to corresponding coroutine
  inline static int encode_wrid(int cor_id,int pending_doorbell = 0) {
    return (pending_doorbell << COR_ID_BIT) | cor_id;
  }

  inline static int decode_corid(int wrid) {
    return wrid & COR_ID_MASK;
  }

  inline static int decode_pending_doorbell(int wrid) {
    return wrid >> COR_ID_BIT;
  }

  void add_pending(int cor_id,rdmaio::Qp *qp);

  // poll all the pending qps of the thread and schedule
  void poll_comps();

  void thread_local_init(int coroutines);

  void report();

  int  *pending_counts_; // number of pending qps per thread

 private:
  std::deque<rdmaio::Qp *> pending_qps_;
  struct ibv_wc wc_;

};

}; // namespace db

};   // namespace nocc

#endif // DB_RDMA_SCHED
