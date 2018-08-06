#pragma once

#include "framework/bench_worker.h"
#include "cluster_chaining.hpp"

namespace nocc {

extern __thread oltp::BenchWorker* worker;

namespace drtm {

template <typename Data, int DRTM_CLUSTER_NUM>
void ClusterHash<Data,DRTM_CLUSTER_NUM>::fetch_node(Qp *qp,uint64_t off,char *buf,int size) {
  qp->rc_post_send(IBV_WR_RDMA_READ,buf,size,off,IBV_SEND_SIGNALED);
  auto res = qp->poll_completion();
  if(res != Qp::IO_SUCC) {
    assert(false);
  }
}

template <typename Data, int DRTM_CLUSTER_NUM>
void ClusterHash<Data,DRTM_CLUSTER_NUM>::fetch_node(Qp *qp,uint64_t off,char *buf,int size,
                                                    nocc::oltp::RScheduler *sched,yield_func_t &yield) {
  int flag = IBV_SEND_SIGNALED;
  if(size < 64) flag |= IBV_SEND_INLINE;

  sched->post_send(qp,worker->cor_id(),
                   IBV_WR_RDMA_READ,buf,size,off,flag);
  worker->indirect_yield(yield);
}

}; // namespace drtm

}; // namespace nocc
