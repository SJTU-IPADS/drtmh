#pragma once

#include "framework/bench_worker.h"
#include "cluster_chaining.hpp"

namespace nocc {

extern __thread oltp::BenchWorker* worker;

namespace drtm {

template <typename Data, int DRTM_CLUSTER_NUM>
void ClusterHash<Data,DRTM_CLUSTER_NUM>::fetch_node(RCQP *qp,uint64_t off,char *buf,int size) {

  qp->post_send(IBV_WR_RDMA_READ,buf,size,off,IBV_SEND_SIGNALED);
  ibv_wc wc = {};
  auto res = qp->poll_till_completion(wc,::rdmaio::no_timeout);
  ASSERT(res == SUCC) << "res " << (int)res;
}

template <typename Data, int DRTM_CLUSTER_NUM>
void ClusterHash<Data,DRTM_CLUSTER_NUM>::fetch_node(RCQP *qp,uint64_t off,char *buf,int size,
                                                    nocc::oltp::RScheduler *sched,yield_func_t &yield) {
  int flag = IBV_SEND_SIGNALED;
  //if(size < 64) flag |= IBV_SEND_INLINE;

  sched->post_send(qp,worker->cor_id(),
                   IBV_WR_RDMA_READ,buf,size,off,flag);
  worker->indirect_yield(yield);
}

}; // namespace drtm

}; // namespace nocc
