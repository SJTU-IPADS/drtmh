#include "rdma_sched.h"
#include "routine.h" // poll_comps will add_routine to scheduler

#include "util/util.h"  // for rdtsc()
#include "logging.h"

#include "rrpc.h"

extern size_t nthreads;
extern size_t current_partition;

using namespace std;
using namespace rdmaio;

namespace nocc {

namespace oltp {

RScheduler::RScheduler() {

}

RScheduler::~RScheduler() {

}

void RScheduler::thread_local_init(int coroutines) {
  pending_counts_ = new int[coroutines + 1];
  std::fill_n(pending_counts_,1 + coroutines,0);
}


void RScheduler::poll_comps() {

  for(auto it = pending_qps_.begin();it != pending_qps_.end();) {

    RCQP *qp = *it;
    auto poll_result = qp->poll_send_completion(wc_);

    if(poll_result == 0) {
      it++;
      continue;
    }

    if(unlikely(wc_.status != IBV_WC_SUCCESS)) {
      LOG(3) << "got bad completion with status: " << wc_.status << " with error " << ibv_wc_status_str(wc_.status)
             << ";@ node " << qp->idx_.node_id;
      if(wc_.status != IBV_WC_RETRY_EXC_ERR)
        assert(false);
      else {
        it++;
        continue;
      }
    }

    static_assert(sizeof(wc_.wr_id) == sizeof(uint64_t),"Un supported wr_id size!");
    uint64_t low_watermark = decode_watermark(wc_.wr_id);

    ASSERT(low_watermark > qp->low_watermark_) << "encoded watermark: " << low_watermark
                                               << "; current watermark: " << qp->low_watermark_;
    qp->low_watermark_ = low_watermark;

    auto cor_id = decode_corid(wc_.wr_id);

    if(cor_id == 0)
      continue;  // ignore null completion

    //LOG(2) << "polled " << cor_id  << " low " << low_watermark;

    ASSERT(pending_counts_[cor_id] > 0) << "cor id " << cor_id
                                        << "; pendings " << pending_counts_[cor_id];
    pending_counts_[cor_id] -= 1;

    if(pending_counts_[cor_id] == 0 && RRpc::reply_counts_[cor_id] == 0) {
      add_to_routine_list(cor_id);
    }

    // update the iterator
    it = pending_qps_.erase(it);
  }
}

void RScheduler::report() {
}

__thread int *RScheduler::pending_counts_ = NULL;


} // namespace db

}   // namespace nocc
