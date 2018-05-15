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
 * RTM profiling - Hao Qian
 * RTM profiling - XingDa
 * 
 */


#ifndef DRTM_UTIL_TXPROFILE_H_
#define DRTM_UTIL_TXPROFILE_H_

#include <immintrin.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>


#define XBEGIN_STARTED_INDEX 0
#define XABORT_EXPLICIT_INDEX 1
#define XABORT_SYSTEM_INDEX 2
#define XABORT_CONFLICT_INDEX 3
#define XABORT_CAPACITY_INDEX 4
#define XABORT_DEBUG_INDEX 5
#define XABORT_EXPLICIT_INNER_INDEX 6
#define XABORT_SYSTEM_INNER_INDEX 7
#define XABORT_CONFLICT_INNER_INDEX 9
#define XABORT_CAPACITY_INNER_INDEX 10
#define XABORT_DEBUG_INNER_INDEX 11
#define XABORT_MANUAL_COUNT 12
#define XABORT_RDMA_INNER_INDEX 13


class RTMProfile {

 public:

  pthread_spinlock_t slock;
  uint32_t status[XABORT_RDMA_INNER_INDEX + 1];
  uint32_t abortCounts;
  uint32_t capacityCounts;
  uint32_t conflictCounts;
  uint32_t succCounts;
  uint32_t lockCounts;
  uint32_t explicitAbortCounts;
  uint32_t nestedCounts;

  //add fallback counting
  uint32_t total_counts;
  uint32_t fallback_counts;

  static __inline__ void atomic_inc32(uint32_t *p)
  {
    __asm__ __volatile__("lock; incl %0"
			 : "+m" (*p)
			 :
			 : "cc");
  }

  static __inline__ void atomic_add32(uint32_t *p, int num)
  {
    __asm__ __volatile__("lock; addl %1, %0"
			 : "+m" (*p)
			 : "q" (num)
			 : "cc");
  }

  RTMProfile() {
    pthread_spin_init(&slock, PTHREAD_PROCESS_PRIVATE);
    abortCounts	= 0;
    succCounts = 0;
    capacityCounts = 0;
    conflictCounts = 0;
    lockCounts = 0;
    for( int i = 0; i < 7; i++)
      status[i] =0;
  }

  void recordLock() {
    atomic_inc32(&lockCounts);
  }

  void recordRetryNum(int num) {

    atomic_add32(&abortCounts, num);
    atomic_inc32(&succCounts);
  }

  void start_record() {
    atomic_inc32(&total_counts);
  }

  void fallback_record() {
    atomic_inc32(&fallback_counts);
  }

  void recordAbortStatus(int stat) {
    atomic_inc32(&abortCounts);
    if((stat & _XABORT_CAPACITY) != 0)  {

      if((stat & _XABORT_NESTED))
	atomic_inc32(&status[XABORT_CAPACITY_INNER_INDEX]);
      else
	atomic_inc32(&status[XABORT_CAPACITY_INDEX]);

    }
    else if((stat & _XABORT_CONFLICT) != 0) {

      if((stat & _XABORT_NESTED))
	atomic_inc32(&status[XABORT_CONFLICT_INNER_INDEX]);
      else
	atomic_inc32(&status[XABORT_CONFLICT_INDEX]);
    }
    else if((stat & _XABORT_DEBUG) != 0) {

      if((stat & _XABORT_NESTED))
	atomic_inc32(&status[XABORT_DEBUG_INNER_INDEX]);
      else
	atomic_inc32(&status[XABORT_DEBUG_INDEX]);

    }
    else if((stat & _XABORT_EXPLICIT) != 0){

      if((stat & _XABORT_NESTED))
	atomic_inc32(&status[XABORT_EXPLICIT_INNER_INDEX]);
      if((_XABORT_CODE(stat)  == 0x73))
	atomic_inc32(&status[XABORT_EXPLICIT_INDEX]);
      if((_XABORT_CODE(stat) == 0x93))
	atomic_inc32(&status[XABORT_MANUAL_COUNT]);

    } else if ((stat &  _XABORT_NESTED) != 0)  {
      atomic_inc32(&status[XABORT_SYSTEM_INNER_INDEX]);

    } else if (stat == 0) {
      atomic_inc32(&status[XABORT_SYSTEM_INDEX]);
    }
  }


  void MergeLocalStatus(RTMProfile& stat) {


  }

  void localRecordAbortStatus(int stat) {

  }

  void reportAbortStatus() {

    if(status[XBEGIN_STARTED_INDEX] != 0)
      printf("XBEGIN_STARTED %d\n", status[XBEGIN_STARTED_INDEX]);
    if(status[XABORT_EXPLICIT_INDEX] != 0)
      printf("XABORT_EXPLICIT %d\n", status[XABORT_EXPLICIT_INDEX]);
    if(status[XABORT_CONFLICT_INDEX] != 0)
      printf("XABORT_CONFLICT %d\n", status[XABORT_CONFLICT_INDEX]);
    if(status[XABORT_CAPACITY_INDEX] != 0)
      printf("XABORT_CAPACITY %d\n", status[XABORT_CAPACITY_INDEX]);
    if(status[XABORT_DEBUG_INDEX] != 0)
      printf("XABORT_ZERO %d\n", status[XABORT_DEBUG_INDEX]);
    if(status[XABORT_SYSTEM_INDEX] != 0)
      printf("XABORT_SYSTEM %d\n", status[XABORT_SYSTEM_INDEX]);
    if(status[XABORT_EXPLICIT_INNER_INDEX] != 0)
      printf("XABORT_INNER_EXPLICIT %d\n", status[XABORT_EXPLICIT_INNER_INDEX]);
    if(status[XABORT_CONFLICT_INNER_INDEX] != 0)
      printf("XABORT_INNER_CONFLICT %d\n", status[XABORT_CONFLICT_INNER_INDEX]);
    if(status[XABORT_CAPACITY_INNER_INDEX] != 0)
      printf("XABORT_INNER_CAPACITY %d\n", status[XABORT_CAPACITY_INNER_INDEX]);
    if(status[XABORT_DEBUG_INNER_INDEX] != 0)
      printf("XABORT_INNER_ZERO %d\n", status[XABORT_DEBUG_INNER_INDEX]);
    if(status[XABORT_SYSTEM_INNER_INDEX] != 0)
      printf("XABORT_SYSTEM_INNER %d\n", status[XABORT_SYSTEM_INNER_INDEX]);

    if(status[XABORT_MANUAL_COUNT] != 0)
      printf("tx abort counts %d\n", status[XABORT_MANUAL_COUNT]);

    if(lockCounts != 0) printf("ACQUIRE LOCK %d\n", lockCounts);

    if(abortCounts != 0 ){
      printf("Abort Counts %d\n", abortCounts);
      printf("Succ Counts %d\n", succCounts);
      printf("Capacity Counts %d\n", capacityCounts);
      printf("Conflict Counts %d\n", conflictCounts);
    }

    printf("total executed: %d\n",total_counts);
    printf("fallback execited: %d\n",fallback_counts);
    if(total_counts != 0)
      printf("ratio: %f\n",(float)fallback_counts / total_counts);

  }

  ~RTMProfile() { pthread_spin_destroy(&slock);}

 private:
  RTMProfile(const RTMProfile&);
  void operator=(const RTMProfile&);
};



#endif  // STORAGE_LEVELDB_UTIL_TXPROFILE_H_
