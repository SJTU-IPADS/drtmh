#ifndef MEMSTOREHASH_H
#define MEMSTOREHASH_H

#include <stdlib.h>
#include <iostream>

//#include "util/rtmScope.h" 
#include "util/rtm.h" 
//#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "port/port_posix.h"
#include "memstore.h"

#define HASH_LOCK 0
#define HASHLENGTH 256*1024*1024

namespace leveldb {

class MemstoreHashTable: public Memstore {

public:	
	struct HashNode {
		//The first field should be next 
		HashNode* next;
		uint64_t key;
		MemNode memnode;
	};

	struct Head{
		HashNode *h;
		//char padding[56];
	};
	
	int length;
	char padding0[64];
	Head *lists;
	char padding1[64];
	RTMProfile prof;
	char padding2[64];
	SpinLock rtmlock;
	char padding3[64];
	port::SpinLock slock;
	char padding4[64];
	static __thread HashNode *dummynode_;
	
	MemstoreHashTable(){
		length = HASHLENGTH;
		lists = new Head[length];
		for (int i=0; i<length; i++)
			lists[i].h = NULL;
		
	}
	
	~MemstoreHashTable(){
//		printf("=========HashTable========\n");
//		PrintStore();
		prof.reportAbortStatus();
	}
	
	inline void ThreadLocalInit() {
		if(dummynode_ == NULL) {
			dummynode_ = new HashNode();
		}
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
	
	static inline uint64_t GetHash(uint64_t key) {
		return MurmurHash64A(key, 0xdeadbeef) & (HASHLENGTH - 1);
		//return key % HASHLENGTH ;
	}


	inline MemNode* Put(uint64_t key, uint64_t* val) {
		ThreadLocalInit();
		
		MemNode* res = GetWithInsert(key);

		res->value = val;
		
		return res;
	}
	
	inline MemNode* GetWithInsert(uint64_t key) {
		
		ThreadLocalInit();
		
		MemNode* mn = Insert_rtm(key);
		
		return mn;
		
	}

	inline MemNode* _GetWithInsert(uint64_t key,char *val) {
	  //	  assert(false);
	  //ThreadLocalInit();
		//dummynode_->value = (uint64_t *)val;		
	  //	MemNode* mn = Insert_rtm(key);
	  //		mn->value = (uint64_t *)val;
	  //		return mn;
	  //	  return Put(key,(uint64_t *)val);
	  return Get(key);		
	}	


	inline MemNode* Get(uint64_t key) {
		
		uint64_t hash = GetHash(key);
		
		HashNode* cur = lists[hash].h;

		while(cur != NULL && cur->key < key) {
			cur = cur->next;
		}

		if(cur != NULL && cur->key == key)
			return &cur->memnode;

		return NULL;
	}

	inline MemNode* Insert_rtm(uint64_t key) {
		uint64_t hash = GetHash(key);
	
#if HASH_LOCK		
		MutexSpinLock lock(&slock);		
#else
		RTMScope begtx(&prof, 1, 1, &rtmlock);
//		RTMArenaScope begtx(&rtmlock, &prof, NULL);
#endif

		HashNode* prev = (HashNode *)&lists[hash];
		HashNode* cur = prev->next;
		
		while(cur != NULL && cur->key < key) {
			prev = cur;
			cur = cur->next;
		}

		if(cur != NULL && cur->key == key)
		  return &cur->memnode;

		
		prev->next = dummynode_;
		dummynode_->next = cur;
		
		dummynode_->key = key;
					
		MemNode* res = &dummynode_->memnode;
		dummynode_ = NULL;
		
		return res;
	}

	void PrintStore() {
		int total = 0;
		for (int i=0; i<length; i++) {
			int count = 0;
			if (lists[i].h != NULL)  {
				//printf("Hash %ld :\t", i);
				HashNode *n = lists[i].h;
				while (n!= NULL) {
					count++;
					//printf("%ld \t", n->key);
					n = n->next;
					total++;
				}
				if (count > 10) printf(" %d\n" , count);
				//printf("\n");
			}
		}

		printf("Total Count %d\n", total);
	}
	 
};
}

#endif
