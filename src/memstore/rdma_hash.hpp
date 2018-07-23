#pragma once

#include "tx_config.h"

#include "cluster_chaining.hpp"
#include "cluster_chaining_remote_op.hpp"
#include "memstore.h"

#include "core/logging.h"
#include "util/util.h"

#include <math.h>

extern size_t total_partition;

namespace nocc {
#define DRTM_CLUSTER_NUM 4
#define CACHE_BUCKET_NUM 4

// a wrapper over cluster_chaining which implements MemStore
typedef  drtm::ClusterHash<uint64_t,CACHE_BUCKET_NUM> loc_cache_t;
class RHash : public Memstore, public drtm::ClusterHash<MemNode,DRTM_CLUSTER_NUM>  {
 public:
  RHash(int expected_data, char *ptr) : drtm::ClusterHash<MemNode,DRTM_CLUSTER_NUM> (expected_data, ptr) {
#if RDMA_CACHE
    uint64_t expected_cached_num = ceil((double)(expected_data) / CACHE_BUCKET_NUM);
    loc_cache_ = new loc_cache_t(2 * expected_cached_num * total_partition);
    LOG(2) << "Cache size: " << get_memory_size_g(loc_cache_->size()) << "G";
#endif
  }

  MemNode *_GetWithInsert(uint64_t key,char *val) {
    MemNode *node = get_with_insert(key);
    if(unlikely(node->value == NULL))
      node->value = (uint64_t *)val;
    return node;
  }

  MemNode *Get(uint64_t key) {
    return get(key);
  }

  MemNode *Put(uint64_t key,uint64_t *val) {
    return _GetWithInsert(key,(char *)val);
  }

  uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp,
                          nocc::oltp::RDMA_sched *sched, yield_func_t &yield,char *val) {
#if RDMA_CACHE
    auto res = *(loc_cache_->get(key));
    return res;
#else
    return remote_get(key,qp,sched,yield,val);
#endif
  }

  uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp,
                          char *val) {
    auto res = remote_get(key,qp,val);
#if RDMA_CACHE
    auto ptr = loc_cache_->get_with_insert(key);
    MemNode *node = (MemNode *)val;
    //*ptr = res; // TODO, now only cache the MemNode's offset!
    *ptr = node->off;
    return res;
#endif
  }
 private:
#if RDMA_CACHE
  loc_cache_t *loc_cache_;
#endif
};
}; // namespace nocc
