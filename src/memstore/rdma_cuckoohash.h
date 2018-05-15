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
 *  Cuckoo hash table using RDMA  - JiaXin
 */ 


#ifndef RDMACUCKOOHASH_H
#define RDMACUCKOOHASH_H

#include <stdlib.h>
#include <iostream>
#include <vector>

//paddings for RDMA,may not be needed

#define SLOT_PER_BUCKET 4
#define MAX_TRY 500
using std::vector;
namespace drtm {

  class RdmaCuckooHash {

  public:
    struct RdmaArrayNode {
      uint64_t key;
      uint64_t index;
      bool valid;
    };
    char * array;
    int length;
    int entrysize;
    int bucketlength;
    int bucketsize;
    int size;//total
    int data_offset;
    int free_ptr;
    RdmaArrayNode * header;
    RdmaCuckooHash(int esize,int len,char* arr){
      entrysize = (((esize-1)>>3)+1) <<3; ;
      bucketsize = sizeof(RdmaArrayNode) * SLOT_PER_BUCKET;
      length = len ;
      bucketlength = length / SLOT_PER_BUCKET;
      array=arr;
      data_offset = bucketlength * bucketsize;
      free_ptr=0;
      size = data_offset+ entrysize * length ;

      header = (RdmaArrayNode *)array;
    }

    ~RdmaCuckooHash(){
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
      return MurmurHash64A(key, 0xdeadbeef) % bucketlength;
    }

    inline uint64_t GetHash2(uint64_t key) {
      return key % bucketlength ;
    }
    
    uint64_t get_dataloc(int index){
      return data_offset+entrysize * index;
    }
    void find_path(uint64_t start_pos,vector<uint64_t>& pos_vec){
      int depth=0;
      uint64_t kick_pos=start_pos;
      while(depth<MAX_TRY){
	RdmaArrayNode * node =&header[kick_pos];
	uint64_t kick_key=node->key;
	pos_vec.push_back(kick_pos);
	uint64_t another_hash=(GetHash(kick_key)==(kick_pos/SLOT_PER_BUCKET)?GetHash2(kick_key):GetHash(kick_key));
	depth++;
	for(int i=0;i<SLOT_PER_BUCKET;i++){
	  node =&header[another_hash*SLOT_PER_BUCKET+i];
	  if(node->valid==false){
	    // find a empty slot
	    pos_vec.push_back(another_hash*SLOT_PER_BUCKET+i);
	    return ;
	  }
	}
	kick_pos=another_hash*SLOT_PER_BUCKET+ rand()%SLOT_PER_BUCKET;
      }
      assert(false);
    }

    void Insert(uint64_t key, void* val) {
      uint64_t p[2];
      p[0]=GetHash(key);
      p[1]=GetHash2(key);
      for(int slot=0;slot<2;slot++){
	for(int i=0;i<SLOT_PER_BUCKET;i++){
	  RdmaArrayNode * node = &header[p[slot]*SLOT_PER_BUCKET+i];
	  if(node->valid==false){
	    node->valid=true;
	    node->key=key;
	    node->index=free_ptr;
	    memcpy((void*)(array+get_dataloc(free_ptr)),val,entrysize);
	    free_ptr ++;
	    return ;
	  }
	}
      }

      //// didn't find empty slot at first
      uint64_t kick_pos=(rand()%2==0? p[0]:p[1])*SLOT_PER_BUCKET+ rand()%SLOT_PER_BUCKET;
      vector<uint64_t> pos_vec;
      find_path(kick_pos,pos_vec);
      int pointer=pos_vec.size()-1;
      RdmaArrayNode * node = &header[pos_vec[pointer]];
      node->valid=true;
      while(pointer>0){
	RdmaArrayNode * prev =&header[pos_vec[pointer-1]];
	node->key=prev->key;
	node->index=prev->index;
	//            memcpy((void*)(node+1),(void*)(prev+1),entrysize);
	node=prev;
	pointer--;
      }
      node->key=key;
      node->index=free_ptr;
      memcpy((void*)(array+get_dataloc(free_ptr)),val,entrysize);
      free_ptr ++;
      return;
    }


    uint64_t* Get(uint64_t key) {
      uint64_t p[2];
      p[0]=GetHash(key);
      p[1]=GetHash2(key);
      for(int slot=0;slot<2;slot++){
	for(int i=0;i<SLOT_PER_BUCKET;i++){
	  char* bucket_addr= array+p[slot]*bucketsize;
	  RdmaArrayNode * node =&header[p[slot]*SLOT_PER_BUCKET+i];
	  if(node->valid==true && node->key==key){
	    return (uint64_t*)(array+get_dataloc(node->index));
	  }
	}
      }
      return NULL;
    }

    void* Delete(uint64_t key) {
      // TODO
      return NULL;
    }
  };
}
#endif
