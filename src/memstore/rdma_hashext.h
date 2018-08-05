#ifndef DRTM_MEM_RDMAHASHEXT_H
#define DRTM_MEM_RDMAHASHEXT_H

#include "tx_config.h"

#include "util/rtm.h"
#include "util/util.h"
#include "memstore.h"

#include "rdmaio.h" // for qp operation
#include "ralloc.h" // for Rmalloc

#include "framework/bench_worker.h"

//#include "sparsehash/dense_hash_map"


#include <stdlib.h>
#include <iostream>
#include <limits>
#include <unordered_map>

#define HASH_LOCK 0

//paddings for RDMA,may not be needed
#define MAX_THREADS  16

#define CLUSTER_H    8

using namespace rdmaio;
//using google::dense_hash_map;      // namespace where class lives by default

namespace nocc {
  extern __thread oltp::BenchWorker* worker;

namespace drtm {

  namespace memstore {

    class RdmaHashExt : public Memstore {

    public:
      struct HeaderNode {
        uint64_t next;
        uint64_t keys[CLUSTER_H];
        uint64_t indexes[CLUSTER_H];
      };
      struct DataNode {
        uint64_t key;
        bool valid;
      };
      char * array;
      uint length;
      uint Logical_length;
      uint indirect_length;
      uint total_length;
      uint entrysize;
      uint header_size;
      uint data_size;
      uint64_t size;
      uint64_t free_indirect;
      uint64_t free_data;

      uint64_t base_off;
      char    *base_ptr;

#if CACHING == 1
      //std::map<uint64_t, uint64_t> loc_cache;
      //dense_hash_map<uint64_t,uint64_t> loc_cache;
      std::unordered_map<uint64_t,uint64_t > loc_cache;
#endif

      RdmaHashExt(int len,char* arr){

        //entrysize = (((esize-1)>>3)+1) <<3;
        entrysize = sizeof(MemNode);
        // round up
        entrysize = (((entrysize - 1) >> 3) + 1) << 3;

        // max number of data in the table
        length = len ;

        Logical_length  = length * 1/CLUSTER_H;
        indirect_length = length * 1/2;
        total_length  = length + indirect_length;
        free_indirect = Logical_length;
        free_data = indirect_length;
        header_size = sizeof(HeaderNode);
        data_size = (sizeof(DataNode) + entrysize);
        size = indirect_length * header_size +  length * data_size;
        if(arr != NULL) {
          array = arr;
        } else {
          // uint64_t buf_size = 1024 * total_length;
          uint64_t G = 1024* 1024 * 1024;
          uint64_t buf_size = 8 * G;
          array = (char *)malloc(buf_size);
        }
#if CACHING == 1
        //loc_cache.set_empty_key(std::numeric_limits<uint64_t>::max());
#endif
      }

      ~RdmaHashExt(){
      }

      void enable_remote_accesses(RdmaCtrl *cm) {
        base_ptr = (char *)(cm->conn_buf_);
        assert((uint64_t)array > (uint64_t)base_ptr);
        base_off = array -  base_ptr;
      }

      // simple RDMA helper function
      inline void fetch_node(Qp *qp,uint64_t off,char *buf,int size) {
        qp->rc_post_send(IBV_WR_RDMA_READ,buf,size,off,IBV_SEND_SIGNALED);
        auto res = qp->poll_completion();
        if(res != Qp::IO_SUCC) {
          assert(false);
        }
      }

      inline void fetch_node(Qp *qp,uint64_t off,char *buf,int size,
                             nocc::oltp::RScheduler *sched,yield_func_t &yield) {
        qp->rc_post_send(IBV_WR_RDMA_READ,buf,size,off,IBV_SEND_SIGNALED,worker->cor_id());
        sched->add_pending(worker->cor_id(),qp);
        worker->indirect_yield(yield);
      }

      virtual uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp,
                                      nocc::oltp::RScheduler *sched,yield_func_t &yield) {

#if CACHING == 1
        if(loc_cache.find(key) != loc_cache.end())
          return loc_cache[key];
        assert(false);
#endif

        uint64_t index = GetHash(key);
        uint64_t loc = getHeaderNode_loc(index) + base_off;

        char *exch = (char *)Rmalloc(256);

        // send an read
        fetch_node(qp,loc,exch,sizeof(RdmaHashExt::HeaderNode),sched,yield);

        RdmaHashExt::HeaderNode* node = (RdmaHashExt::HeaderNode*) exch;

        while (true) {
          for (int i = 0; i < CLUSTER_H; i++) {
            if (node->keys[i] == key && node->indexes[i] != 0) {
              loc = getDataNode_loc(node->indexes[i]);
              loc += sizeof(DataNode);
              fetch_node(qp,loc + base_off,exch,sizeof(MemNode),sched,yield);
              MemNode *node = (MemNode *)(exch);
              auto res = node->off;
              Rfree(exch);
              return res;
            }
          }

          if (node->next != 0) {
            // fetch next
            loc = getHeaderNode_loc(node->next) + base_off;
            fetch_node(qp,loc,exch,sizeof(RdmaHashExt::HeaderNode),sched,yield);
          }
          else {
            fprintf(stderr,"Remote key traverse error yield, key %lu\n",key);
            assert(false);
            return 0;
          }
        }
      end:
        assert(false);
        return 0;

      }

      virtual uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp) {

#if CACHING == 1
        if(loc_cache.find(key) != loc_cache.end())
          return loc_cache[key];
#endif

        uint64_t index = GetHash(key);
        uint64_t loc = getHeaderNode_loc(index) + base_off;

        char *exch = (char *)Rmalloc(256);

        // send an read
        fetch_node(qp,loc,exch,sizeof(RdmaHashExt::HeaderNode));

        RdmaHashExt::HeaderNode* node = (RdmaHashExt::HeaderNode*) exch;

        while (true) {
          for (int i = 0; i < CLUSTER_H; i++) {
            if (node->keys[i] == key && node->indexes[i] != 0) {
              loc = getDataNode_loc(node->indexes[i]);
              loc += sizeof(DataNode);
              fetch_node(qp,loc + base_off,exch,sizeof(MemNode));
              MemNode *node = (MemNode *)(exch);
              auto res = node->off;
              Rfree(exch);
#if CACHING == 1
              assert(loc_cache.find(key) == loc_cache.end());
              loc_cache.insert(std::make_pair(key,res));
#endif
              return res;
            }
          }

          if (node->next != 0) {
            // fetch next
            loc = getHeaderNode_loc(node->next) + base_off;
            fetch_node(qp,loc,exch,sizeof(RdmaHashExt::HeaderNode));
          }
          else {
            fprintf(stderr,"remote key traverse error, key %lu\n",key);
            assert(false);
            return 0;
          }
        }
      end:
        assert(false);
        return 0;
      }

      static inline uint64_t MurmurHash64A (uint64_t key, unsigned int seed )  {

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

      inline uint64_t GetHash(uint64_t key) {
        return MurmurHash64A(key, 0xdeadbeef) % Logical_length;
      }

      inline DataNode * getDataNode(int i){
        return (DataNode *)(array+ getDataNode_loc(i));
      }

      inline uint64_t getDataNode_loc(int i){
        return indirect_length * header_size +  (i-indirect_length) * data_size;
      }

      inline uint64_t getHeaderNode_loc(int i){
        return i*header_size;
      }

      void Insert(uint64_t key, void* val) {
        if(free_data == total_length){
          //	printf("fail when inserting %lldd\n",key);
          assert(false);
        }
        uint64_t hash = GetHash(key);
        HeaderNode * node =(HeaderNode *) (array+hash*header_size);
        while(node->next !=0){
          node =(HeaderNode *) (array+(node->next)*header_size);
        }
        for(int i=0;i<CLUSTER_H;i++){
          if(node->indexes[i]==0){
            DataNode * free_node=getDataNode(free_data);
            free_node->key=key;
            memcpy((void*)(free_node+1),val,entrysize);
            node->keys[i]=key;
            node->indexes[i]=free_data;
            free_data ++ ;
            return ;
          }
        }
        if(free_indirect == indirect_length){
          //	printf("fail when allocating indirect node,key is %lld\n",key);
          assert(false);
        }
        node->next = free_indirect;
        node =(HeaderNode *) (array+free_indirect*header_size);
        free_indirect++;
        DataNode * free_node=getDataNode(free_data);
        free_node->key=key;
        memcpy((void*)(free_node+1),val,entrysize);
        node->keys[0]=key;
        node->indexes[0]=free_data;
        free_data ++ ;
        return;
      }

      MemNode* Get(uint64_t key) {
        return _GetWithInsert(key,NULL);
#if 0
        uint64_t hash = GetHash(key);
        HeaderNode * node =(HeaderNode *) (array+hash*header_size);
        while(true){
          for(int i=0;i<CLUSTER_H;i++){
            if(node->keys[i]==key && node->indexes[i]!=0){
              DataNode* datanode=getDataNode(node->indexes[i]);
              return (uint64_t*)(datanode+1);
            }
          }
          if(node->next != 0)
            node = (HeaderNode *) (array+(node->next)*header_size);
          else{
            assert(false);
          }
        }
#endif
      }

      uint64_t read(uint64_t key) {
        uint64_t hash = GetHash(key);
        HeaderNode * node =(HeaderNode *) (array+hash*header_size);
        int count=0;
        while(true){
          count++;
          for(int i=0;i<CLUSTER_H;i++){
            if(node->keys[i]==key && node->indexes[i]!=0){
              DataNode* datanode=getDataNode(node->indexes[i]);
              return count;
            }
          }
          if(node->next != 0)
            node = (HeaderNode *) (array+(node->next)*header_size);
          else{
            assert(false);
          }
        }
      }

      virtual MemNode* Put(uint64_t k, uint64_t* val) {
        return _GetWithInsert(k,(char *)val);
      }

      virtual MemNode *_GetWithInsert(uint64_t key,char *val) {

        uint64_t index = GetHash(key);
        uint64_t loc   = getHeaderNode_loc(index);

        MemNode *res = NULL;

        HeaderNode* node = (HeaderNode*)(array + loc);

#if CLUSTER_LOCK
        //MutexSpinLock lock(&hash_lock);
#else
        //      RTMScope tx(NULL, 1, 1, &hash_lock);
#endif

        while(1) {

          for (uint i = 0; i < CLUSTER_H; i++) {
            if (node->keys[i] == key && node->indexes[i] != 0) {
              loc = getDataNode_loc(node->indexes[i]);
              loc += sizeof(DataNode);
              res =  (MemNode *)(array + loc);
              goto GET_INSERT_RET;
            }
          }

          if (node->next != 0 ) {
            // jump to another node
            loc = getHeaderNode_loc(node->next);
            node = (HeaderNode *) (array + loc);

          } else {
            // get failed ,start inserting
            if(free_data == total_length) {
              fprintf(stdout,"no free space for insertion!\n");
              assert(false);
              goto GET_INSERT_RET;
            }
            for(uint i = 0;i < CLUSTER_H;++i) {
              if(node->indexes[i] == 0) {
                // find the slot entry
                DataNode *free_node = getDataNode(free_data);
                free_node->key = key;
                node->keys[i] = key;
                node->indexes[i] = free_data;
                free_data++;
                res = (MemNode *)((char *)free_node + sizeof(DataNode));
                res->value = (uint64_t *)val;
#if ONE_SIDED == 1 // record the off
                // if the one-sided operations are enabled, then we will record the offset
                res->value = (uint64_t *)val;
                res->off   = ((char *)val - (char *)base_ptr); // store the offset in the node object
#endif
                goto GET_INSERT_RET;
              }
            }

            // donot find the slot, create one
            if(free_indirect == indirect_length) {
              fprintf(stderr,"fail when allocating indirect node");
              exit(-1);
            }
            node->next = free_indirect;
            node = (HeaderNode *)(array + free_indirect * header_size);
            free_indirect++;
            DataNode * free_node = getDataNode(free_data);
            free_node->key = key;
            node->keys[0] = key;
            node->indexes[0] = free_data;
            free_data++ ;
            res =  (MemNode *)((char *)free_node + sizeof(DataNode));
#if ONE_SIDED == 1
            // if the one-sided operations are enabled, then we will record the offset
            res->value = (uint64_t *)val;
            res->off   = ((char *)val - base_ptr); // store the offset in the node object
            assert(res->off != 0);
#else
            // otherwise, record the address
            res->value = (uint64_t *)val;
#endif
            goto GET_INSERT_RET;
            // end insertion
          }

          // end while(1)
        }

      GET_INSERT_RET:
#if 0
        data_inserted += 1;
#endif
        return res;
      }



      void* Delete(uint64_t key) {
        assert(false);
        return NULL;
      }
    };
  }
};
}; // namespace nocc
#endif
