#pragma once

#include "cluster_chaining.hpp"
#include "memstore.h"
#include "tx_config.h"

extern size_t total_partition;

namespace nocc {
#define DRTM_CLUSTER_NUM 4
  // a wrapper over cluster_chaining which implements MemStore

  typedef  drtm::ClusterHash<uint64_t,2> loc_cache_t;
  class RHash : public Memstore, public drtm::ClusterHash<MemNode,DRTM_CLUSTER_NUM>  {
  public:
    RHash(int expected_data, char *ptr) : drtm::ClusterHash<MemNode,DRTM_CLUSTER_NUM> (expected_data, ptr) {
#if RDMA_CACHE
      loc_cache_ = new loc_cache_t(expected_data * total_partition);
#endif
    }

    MemNode *_GetWithInsert(uint64_t key,char *val) {
      MemNode *node = get_with_insert(key);
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
      auto res = loc_cache_->get(key);
      assert(res != NULL);
      fetch_node(qp,*res,val,sizeof(MemNode),sched,yield);
      //auto res1 = RemoteTraverse(key,qp,val);
      //assert(res1 == *res);
      return *res;
#else
      return remote_get(key,qp,sched,yield,val);
#endif
    }

    uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp,
                            char *val) {
      auto res = remote_get(key,qp,val);
#if RDMA_CACHE
      auto ptr = loc_cache_->get_with_insert(key);
      assert(res != 0);
      *ptr = res;
      loc_cache.insert(std::make_pair(key,res));
      return res;
#endif
    }
  private:
#if RDMA_CACHE
    loc_cache_t *loc_cache_;
    std::unordered_map<uint64_t,uint64_t > loc_cache;
#endif
  };
}; // namespace nocc
