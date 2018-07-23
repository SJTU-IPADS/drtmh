#pragma once

#include <cassert>

#include "memstore.h"

#include "rdmaio.h" // for QP operations
#include "ralloc.h" // for RDMA mallocs

#include "core/rdma_sched.h"
#include "core/routine.h"

#include "util/util.h"

namespace nocc {

namespace drtm {

using namespace rdmaio;
using namespace util;

// number of nodes in a header
//#define DRTM_CLUSTER_NUM 4
// return the ith entry off in the node
#define CLUSTER_OFF(node,i) ( (char *)(&(((node))->datas[(i)])) - (char *)node)

// Limitation: current only supports int type key
template <typename Data, int DRTM_CLUSTER_NUM>
class ClusterHash {
 public:
  struct Key {
    uint64_t valid : 1;
    uint64_t key   : 63;
  };

  struct HeaderNode {
    Key keys[DRTM_CLUSTER_NUM];
    Data datas[DRTM_CLUSTER_NUM];
    int32_t next;
  };

  ClusterHash(int expected_data, char *val = NULL)
      :data_num_(expected_data),
       indirect_num_(expected_data <= DRTM_CLUSTER_NUM ? expected_data : expected_data / DRTM_CLUSTER_NUM),
       logical_num_(expected_data), // assume 1 hit
       free_indirect_num_(1),       // we omit the first indirect node
       data_ptr_(val),
       base_off_(0)
  {
    assert(indirect_num_ > 0);
    size_ = (indirect_num_ + logical_num_) * sizeof(HeaderNode);

    // a null hash table
    if(size_ == 0) {
      assert(false);
      return;
    }

    if(data_ptr_ == NULL) {
      //fprintf(stderr,"[CLUSTER hashing warning: ] using self-allocated memory %f!\n",get_memory_size_g(size_));
      if(size_ > HUGE_PAGE_SZ)
        data_ptr_ = (char *)malloc_huge_pages(size_,HUGE_PAGE_SZ,true);
      else
        data_ptr_ = (char *)malloc(size_);
    }
    //fprintf(stdout,"[CLUSTER] Init hash table with size %d\n",size_);
    // zeroing
    assert(data_ptr_ != NULL);
    memset(data_ptr_,0,size_);
  }

  // return the expected size of the hash table
  static int expected_size(int num) {
    int expected_indirct = num / DRTM_CLUSTER_NUM;
    return (expected_indirct + num) * sizeof(HeaderNode);
  }

  int size() const {
    return size_;
  }

  void enable_remote_accesses(RdmaCtrl *cm) {
    auto base_ptr = (char *)(cm->conn_buf_);
    base_off_ = data_ptr_ - base_ptr;
  }

  void fetch_node(Qp *qp,uint64_t off,char *buf,int size);
  void fetch_node(Qp *qp,uint64_t off,char *buf,int size,
                  nocc::oltp::RDMA_sched *sched,yield_func_t &yield);


  inline uint64_t get_indirect_loc(int i) {
    return (i + logical_num_) * sizeof(HeaderNode);
  }

  inline HeaderNode *get_indirect_node(int i) {
    return (HeaderNode *)(data_ptr_ + get_indirect_loc(i));
  }

  inline Data* get(uint64_t key) {

    uint64_t idx = get_hash(key);
    HeaderNode *node = (HeaderNode *)(idx * sizeof(HeaderNode) + data_ptr_);

    while(1) {
      for(uint i = 0;i < DRTM_CLUSTER_NUM;++i) {
        if(node->keys[i].key == key && node->keys[i].valid)
          return &(node->datas[i]);
      }
      if(node->next != 0){
        node = get_indirect_node(node->next);
      }
      else
        break;
    }
    // failed to found one
    return NULL;
  }

  inline Data *get_with_insert(uint64_t key) {
    Data *res;
    if( (res = get(key)) != NULL)
      return res;
 INSERT:
    return insert(key);
  }

  // a blocking version of remote get
  // return offset: point to the MemNode
  inline uint64_t remote_get(uint64_t key,Qp *qp,char *val) {
    assert(base_off_ != 0);
    HeaderNode *node = (HeaderNode *)Rmalloc(sizeof(HeaderNode));

    uint64_t idx = get_hash(key);
    uint64_t node_off = idx * sizeof(HeaderNode) + base_off_;
    fetch_node(qp,node_off,(char *)node,sizeof(HeaderNode));

    while(1) {
      for(uint i = 0;i < DRTM_CLUSTER_NUM;++i) {
        if(node->keys[i].key == key && node->keys[i].valid) {
          uint64_t res = CLUSTER_OFF(node,i) + node_off;
          memcpy(val,&(node->datas[i]),sizeof(Data));
          Rfree(node);
          return res;
        }
      } // traverse the large header
      assert(node->next >= 0 && indirect_num_ > node->next);
      if(node->next != 0) {
        node_off = get_indirect_loc(node->next) + base_off_;
        fetch_node(qp,node_off,(char *)node,sizeof(HeaderNode));
      } else {
        assert(false);
      }
    }
    Rfree(node);
    return 0; // error case, shall not happen
  }

  //
  inline uint64_t remote_get(uint64_t key, Qp *qp, nocc::oltp::RDMA_sched *sched,yield_func_t &yield,char *val) {

    assert(base_off_ != 0);
    HeaderNode *node = (HeaderNode *)Rmalloc(sizeof(HeaderNode));

    uint64_t idx = get_hash(key);
    uint64_t node_off = idx * sizeof(HeaderNode) + base_off_;
    fetch_node(qp,node_off,(char *)node,sizeof(HeaderNode),sched,yield);

    while(1) {
      for(uint i = 0;i < DRTM_CLUSTER_NUM;++i) {
        if(node->keys[i].key == key && node->keys[i].valid) {
          uint64_t res = CLUSTER_OFF(node,i) + node_off;
          memcpy(val,&(node->datas[i]),sizeof(Data));
          Rfree(node);
          return res;
        }
      }
      if(node->next != 0) {
        node_off = get_indirect_loc(node->next) + base_off_;
        node->next = -1;
        fetch_node(qp,node_off,(char *)node,sizeof(HeaderNode),sched,yield);
        assert(node->next != -1); //reset
      } else {
        fprintf(stderr,"fetch key error %lu, at node %d\n",key,qp->nid);
        assert(false);
      }
    }
    Rfree(node);
    return 0; // error case, shall not happen
  }

  inline Data *insert(uint64_t key) {

    uint64_t idx = get_hash(key);
    HeaderNode *node = (HeaderNode *)(idx * sizeof(HeaderNode) + data_ptr_);

    // find a free slot
    while(1) {
      for(uint i = 0;i < DRTM_CLUSTER_NUM;++i) {
        if(!node->keys[i].valid) {
          node->keys[i].valid = true;
          node->keys[i].key = key;
          return &(node->datas[i]);
        }
      }
      if(node->next != 0) {
        node = get_indirect_node(node->next);
      }
      else
        break;
    }
 ALLOC_NEW:
    assert(free_indirect_num_ < indirect_num_);
    HeaderNode *new_node = get_indirect_node(free_indirect_num_);
    memset(new_node,0,sizeof(HeaderNode));
    new_node->keys[0].key = key;
    new_node->keys[0].valid = true;
    node->next = free_indirect_num_++;
    return &(new_node->datas[0]);
  }

  static inline uint64_t murmur_hash_64a(uint64_t key, unsigned int seed )  {

    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (8 * m);
    const uint64_t * data = &key;
    const uint64_t * end = data + 1;

    while(data != end)  {
      uint64_t k = *data++;
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(8 & 7)   {
      case 7: h ^= uint64_t(data2[6]) << 48;
      case 6: h ^= uint64_t(data2[5]) << 40;
      case 5: h ^= uint64_t(data2[4]) << 32;
      case 4: h ^= uint64_t(data2[3]) << 24;
      case 3: h ^= uint64_t(data2[2]) << 16;
      case 2: h ^= uint64_t(data2[1]) << 8;
      case 1: h ^= uint64_t(data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
  }

  inline uint64_t get_hash(uint64_t key) {
    return murmur_hash_64a(key, 0xdeadbeef) % logical_num_;
  }


 private:
  char *data_ptr_;

  // total logical slots
  int   logical_num_;

  // total indirect slots
  int   indirect_num_;

  // total data slots
  int   data_num_;

  // currently available data slot
  int   free_data_num_;

  // currently available indirect num
  int   free_indirect_num_;

  // the size of the table
  int size_;

  // offset in the RDMA region
 public:
  uint64_t base_off_;
};

} // namespace drtm
} // namespace nocc
