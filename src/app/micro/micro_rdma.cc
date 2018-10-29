#include "tx_config.h"
#include "micro_worker.h"

extern size_t distributed_ratio; // used for some app defined parameters
extern size_t total_partition;
extern size_t current_partition;

using namespace rdmaio;

namespace nocc {
namespace oltp {

extern __thread util::fast_random   *random_generator;
extern char *rdma_buffer;
extern char *free_buffer;

namespace micro {

const uint64_t working_space = 8 * 1024 * 1024;

txn_result_t MicroWorker::micro_rdma_atomic(yield_func_t &yield) {

  int      pid    = random_generator[cor_id_].next() % total_partition;
  uint64_t offset = random_generator[cor_id_].next() % (working_space - sizeof(uint64_t));
  // align
  offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
  uint64_t size = sizeof(uint64_t);
  char *local_buf = (char *)Rmalloc(size);
  auto rc = rdma_sched_->post_cas(qp_vec_[pid],cor_id_,local_buf,offset,0,0,IBV_SEND_SIGNALED);
  ASSERT(rc == SUCC) << "post cas error " << rc << " " << strerror(errno);
  indirect_yield(yield);
  Rfree(local_buf);
  return txn_result_t(true,1);
}

txn_result_t MicroWorker::micro_rdma_write(yield_func_t &yield) {

  ASSERT(working_space <= ((uint64_t)free_buffer - (uint64_t)rdma_buffer))
      << "cannot overwrite RDMA heap's memory, w free area " << get_memory_size_g((uint64_t)free_buffer - (uint64_t)rdma_buffer) << "G";

  auto size = distributed_ratio;
  const int window_size = 1;

  for(uint i = 0;i < window_size;++i) {

    uint64_t off  = random_generator[cor_id_].next() % (working_space - MAX_MSG_SIZE);
    ASSERT((off + size) <= working_space);

 retry:
    int      pid  = random_generator[cor_id_].next() % total_partition;
    {
      char *local_buf = rdma_buffer + worker_id_ * 4096 + cor_id_ * CACHE_LINE_SZ;
      int flag = IBV_SEND_SIGNALED;
      if(size < ::rdmaio::MAX_INLINE_SIZE)
        flag |= IBV_SEND_INLINE;
      auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                       IBV_WR_RDMA_WRITE,local_buf,size,off,flag);
      ASSERT(rc == SUCC) << "post error " << strerror(errno);
    }
  }
  indirect_yield(yield);
  ntxn_commits_ += window_size - 1;

  return txn_result_t(true,1);
}

txn_result_t MicroWorker::micro_rdma_read(yield_func_t &yield) {

  auto size = distributed_ratio;
  const int window_size = 1;

  for(uint i = 0;i < window_size;++i) {

    uint64_t off  = random_generator[cor_id_].next() % working_space;

    if(off + size >= working_space)
      off -= size;

    ASSERT(off <= working_space);
 retry:
    int      pid  = random_generator[cor_id_].next() % total_partition;
    //if(unlikely(pid == current_partition))
    //      goto retry;
    {
      char *local_buf = rdma_buffer + worker_id_ * 4096 + cor_id_ * CACHE_LINE_SZ;
      auto rc = rdma_sched_->post_send(qp_vec_[pid],cor_id_,
                                       IBV_WR_RDMA_READ,local_buf,size,off,
                                       IBV_SEND_SIGNALED);
      ASSERT(rc == SUCC) << "post error " << strerror(errno);
    }
  }
  indirect_yield(yield);
  ntxn_commits_ += window_size - 1;

  return txn_result_t(true,1);
}
} // namespace micro

}

}
