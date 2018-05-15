/*
 *  The code is part of our project called DrTM, which leverages HTM and RDMA for speedy distributed
 *  in-memory transactions.
 *
 *
 * Copyright (C) 2015 Institute of Parallel and Distributed Systems (IPADS), Shanghai Jiao Tong University
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more about this software, visit:  http://ipads.se.sjtu.edu.cn/drtm.html
 *
 */

/*
 *  chain hash table using RDMA - JiaXin
 */ 


#ifndef DRTM_MEM_RDMACHAINHASH_H
#define DRTM_MEM_RDMACHAINHASH_H

#include <stdlib.h>
#include <iostream>

#include "util/rtm.h"

#define HASH_LOCK 0

//paddings for RDMA,may not be needed
#define MAX_THREADS    16

namespace drtm {

  class RdmaChainHash {

  public:
    SpinLock* concurrent_lock;
    struct RdmaArrayNode {
      uint64_t key;
      uint64_t next;
      bool valid;
    };
    char * array;
    int length;
    int header_length;
    int entrysize;
    int slotsize;
    int size;
    uint64_t free_pointer;
    //    uint64_t free_ptrs[MAX_THREADS];
    RdmaChainHash(int esize,int len,char* arr){
      concurrent_lock = new SpinLock();
      entrysize = (((esize-1)>>3)+1) <<3; ;
      slotsize = sizeof(RdmaArrayNode)+entrysize;
      length = len ;
      header_length = length * 1/4;
      free_pointer = header_length;
      size = slotsize * length ;
      array=arr;

      for (uint64_t i=0; i<header_length; i++){
	RdmaArrayNode * node =(RdmaArrayNode *) (array+i*slotsize);
	node->valid=false;
	node->next=0;
      }
      for (uint64_t i=header_length; i<length; i++){
	RdmaArrayNode * node =(RdmaArrayNode *) (array+i*slotsize);
	node->next=i+1;
	if(i==length-1)
	  node->next=0;
      }

      // uint64_t free_length=(length-header_length)/MAX_THREADS;
      // for(int i=0;i<MAX_THREADS;i++){
      //     free_ptrs[i]=header_length+free_length*i;
      // }
    }

    ~RdmaChainHash(){
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
      return MurmurHash64A(key, 0xdeadbeef) % header_length;
      //return key % (_RHASHLENGTH) ;
    }

    void Insert(uint64_t key, void* val) {
      uint64_t hash = GetHash(key);
      RdmaArrayNode * node =(RdmaArrayNode *) (array+hash*slotsize);
      if(node->valid == false){
	node->key=key;
	node->valid=true;
	memcpy((void*)(node+1),val,entrysize);
      } else {
	while(node->next!=0){
	  if(node->key == key){
	    //	    printf("inserting key that is already exist %lld\n",key);
	    return ;
	  }
	  node =(RdmaArrayNode *) (array+(node->next)*slotsize);
	}
	if(free_pointer==length){
	  //	  printf("fail when inserting %lld\n",key);
	  assert(false);
	}
	RdmaArrayNode * free_node=(RdmaArrayNode *) (array+free_pointer*slotsize);
	node->next=free_pointer;
	free_pointer ++ ;//=free_node->next;
	free_node->next=0;
	free_node->key=key;
	memcpy((void*)(free_node+1),val,entrysize);
      }
      return;
    }

    uint64_t* Get(uint64_t key) {
      uint64_t hash = GetHash(key);
      RdmaArrayNode * node =(RdmaArrayNode *) (array+hash*slotsize);
      if(node->valid ==false)
	return NULL;
      while(true){
	if(node->key == key)
	  return (uint64_t*)(node+1);
	else if(node->next==0){
	  return NULL;
	} else {
	  node =(RdmaArrayNode *) (array+(node->next)*slotsize);
	}
      }
    }

    void Concurrent_insert(uint64_t key, void* val) {
      uint64_t hash = GetHash(key);
      RdmaArrayNode * node =(RdmaArrayNode *) (array+hash*slotsize);

      RTMTX::Begin(concurrent_lock);

      if(node->valid == false){
	node->key=key;
	node->valid=true;
	memcpy((void*)(node+1),val,entrysize);
      } else {
	while(node->next!=0){
	  if(node->key == key){
	    //printf("inserting key that is already exist %d\n",key);
	    RTMTX::End(concurrent_lock);
	    return ;
	  }
	  node =(RdmaArrayNode *) (array+(node->next)*slotsize);
	}
	if(free_pointer==length){
	  printf("fail when inserting %ld\n",key);
	  assert(false);
	}
	RdmaArrayNode * free_node=(RdmaArrayNode *) (array+free_pointer*slotsize);
	node->next=free_pointer;
	free_pointer ++ ;//=free_node->next;
	free_node->next=0;
	free_node->key=key;
	memcpy((void*)(free_node+1),val,entrysize);
      }
      RTMTX::End(concurrent_lock);
      return;
    }

    uint64_t* Concurrent_get(uint64_t key) {
      uint64_t hash = GetHash(key);
      RdmaArrayNode * node =(RdmaArrayNode *) (array+hash*slotsize);

      uint64_t* ret;
      RTMTX::Begin(concurrent_lock);
      if(node->valid ==false){
	RTMTX::End(concurrent_lock);
	return NULL;
      }
      while(true){
	if(node->key == key){
	  ret = (uint64_t*)(node+1);
	  break;
	}
	else if(node->next==0){
	  ret = NULL;
	  break;
	} else {
	  node =(RdmaArrayNode *) (array+(node->next)*slotsize);
	}
      }
      RTMTX::End(concurrent_lock);
      return ret;
    }

    void* Delete(uint64_t key) {
      // TODO
      return NULL;
    }
  };
}
#endif
