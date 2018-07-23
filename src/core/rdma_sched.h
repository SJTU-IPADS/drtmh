#ifndef NOCC_RDMA_SCHED
#define NOCC_RDMA_SCHED

#include <deque>

#include "rdmaio.h"

//#include "db_statistics_helper.h"

namespace nocc {

namespace oltp {

class RDMA_sched {
 public:
  RDMA_sched();
  ~RDMA_sched();

  // add pending qp to corresponding coroutine
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
