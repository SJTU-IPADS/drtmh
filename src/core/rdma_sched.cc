#include "rdma_sched.h"
#include "routine.h" // poll_comps will add_routine to scheduler

#include "util/util.h"  // for rdtsc()
#include "logging.h"


extern size_t nthreads;
extern size_t current_partition;

using rdmaio::Qp;
using namespace std;


namespace nocc {

namespace oltp {

RDMA_sched::RDMA_sched() {
}

RDMA_sched::~RDMA_sched() {
}

void RDMA_sched::thread_local_init(int coroutines) {
  pending_counts_ = new int[coroutines + 1];
  for(uint i = 0;i <= coroutines;++i)
    pending_counts_[i] = 0;
}

void RDMA_sched::add_pending(int cor_id, Qp* qp) {
  pending_qps_.push_back(qp);
  pending_counts_[cor_id] += 1;
}

void RDMA_sched::poll_comps() {

  for(auto it = pending_qps_.begin();it != pending_qps_.end();) {

    Qp *qp = *it;
    auto poll_result = ibv_poll_cq(qp->send_cq,1,&wc_);

    if(poll_result == 0) {
      it++;
      continue;
    }

    if(unlikely(wc_.status != IBV_WC_SUCCESS)) {
      LOG(7) << "got bad completion with status: " << wc_.status << " with error " << ibv_wc_status_str(wc_.status)
             << "@node " << qp->nid;
    }

    auto cor_id = wc_.wr_id;
    if(cor_id == 0) continue;  // ignore null completion

    assert(pending_counts_[cor_id] > 0);
    pending_counts_[cor_id] -= 1;

    if(pending_counts_[cor_id] == 0)
      add_to_routine_list(cor_id);

    // update the iterator
    it = pending_qps_.erase(it);
  }
}

void RDMA_sched::report() {
}
} // namespace db
}   // namespace nocc
