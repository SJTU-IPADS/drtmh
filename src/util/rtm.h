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
 *  RTM UTIL         - Hao Qian
 *  RTM UTIL         - XingDa
 */


#ifndef DRTM_UTIL_RTM_H_
#define DRTM_UTIL_RTM_H_
#include <immintrin.h>
#include <sys/time.h>
#include <assert.h>
#include "util/spinlock.h"
#include "txprofile.h"

#define MAXNEST 0
#define NESTSTEP 0
#define MAXZERO 0

//This number is just for performance debugging
//#define MAXCAPACITY 1024
#define MAXCAPACITY 1

#define MAXCONFLICT 100
#define RTMPROFILE 0

#define MAXWRITE 64
#define MAXREAD 128

#define SIMPLERETY 0

typedef volatile uint16_t  __rtm_spin_lock_t;

#define PREFIX_XACQUIRE ".byte 0xF2; "
#define PREFIX_XRELEASE ".byte 0xF3; "

inline unsigned int __try_xacquire(__rtm_spin_lock_t *lock) {
  register unsigned ret = 1;
  //  __asm__ volatile (PREFIX_XACQUIRE "lock; xchgl %0, %1"
  __asm__ volatile ("lock; xchgl  %0, %1"
                    : "=r"(ret),"=m"(*lock):"0"(ret),"m"(*lock):"memory" );

  return (ret^1);
}

inline void Acquire(__rtm_spin_lock_t *lock) {
  int counter = 0;
  for(;;) {
    if( (*lock == 0) && __try_xacquire(lock))
      return;
    counter ++;
    if(counter > 10000) {
      //      fprintf(stdout,"read lock spined %lu\n",*lock);
      exit(-1);
    }
#if 0
    while(*lock) {
      __asm__ volatile ("pause\n" : : : "memory" );
      if( !(*lock != 0 || *lock != 1)) {
        fprintf(stdout,"lock content %lu\n",*lock);
        assert(false);
      }
    }
#endif
  }
}

static inline void Xrelease(__rtm_spin_lock_t *lock) {
  barrier();
  //  __asm__ volatile (PREFIX_XRELEASE "movl $0, %0"
  __asm__ volatile ("movl $0, %0"
                    : "=m"(*lock) : "m"(*lock) : "memory" );

}

class RTMScope {

  RTMProfile localprofile;
  RTMProfile* globalprof;
  int retry;
  int conflict;
  int capacity;
  int nested;
  int zero;
  uint64_t befcommit;
  uint64_t aftcommit;
  SpinLock* slock;

 public:

  static SpinLock fblock;

  inline RTMScope(RTMProfile* prof, int read = 1, int write = 1, SpinLock* sl = NULL) {
    //  inline RTMScope(TXProfile* prof, int read = 1, int write = 1, SpinLock* sl = NULL) {

    //globalprof = prof;
    retry = 0;
    conflict = 0;
    capacity = 0;
    zero = 0;
    nested = 0;

    if(sl == NULL) {
      //If the user doesn't provide a lock, we give him a default locking
      slock = &fblock;
    } else {
      slock = sl;
    }

    while(true) {
      unsigned stat;
      stat = _xbegin();
      if(stat == _XBEGIN_STARTED) {

        //Put the global lock into read set
        if(slock->IsLocked())
          _xabort(0xff);

        return;

      } else {

        retry++;
        //if (prof!= NULL) prof->recordAbortStatus(stat);
        if((stat & _XABORT_NESTED) != 0)
          nested++;
        else if(stat == 0)
          zero++;
        else if((stat & _XABORT_CONFLICT) != 0) {
          conflict++;
        }
        else if((stat & _XABORT_CAPACITY) != 0)
          capacity++;

        if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
          while(slock->IsLocked())
            _mm_pause();
        }
        if((stat & _XABORT_EXPLICIT)) {
          if(prof != NULL) {
            prof->explicitAbortCounts++;
          }
        }
        if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0x73) {
          if(prof != NULL) {
            prof->explicitAbortCounts ++;
          }
          //retry maybe not helpful
          break;
        }

#if SIMPLERETY
        if(retry > 100)
          break;
#else

        int step = 1;

        //		  if((stat & _XABORT_NESTED) != 0)
        //			step = NESTSTEP;
        //break;
        if (nested > MAXNEST)
          break;
        if(zero > MAXZERO/step) {
          break;
        }

        if(capacity > MAXCAPACITY / step) {
          break;
        }
        if (conflict > MAXCONFLICT/step) {
          break;
        }
#endif

      }
    }
    slock->Lock();
    if(prof != NULL) {
#if 0
      prof->succCounts++;
      prof->abortCounts += retry;
      prof->capacityCounts += capacity;
      prof->conflictCounts += conflict;
#endif
      //      prof->nested += nested;
    }
  }

  void Abort() {

    _xabort(0x1);

  }


  inline  ~RTMScope() {
    if(slock->IsLocked())
      slock->Unlock();
    else
      _xend ();
    //    prof->succCounts += 1;

    //access the global profile info outside the transaction scope
#if RTMPROFILE
    if(globalprof != NULL) {
      globalprof->succCounts++;
      globalprof->abortCounts += retry;
      globalprof->capacityCounts += capacity;
      globalprof->conflictCounts += conflict;
    }
#endif
    //		globalprof->MergeLocalStatus(localprofile);

  }

 private:
  RTMScope(const RTMScope&);
  void operator=(const RTMScope&);
};


//Invoke rtm begin and end explicitly
class RTMTX {

 public:
  static RTMProfile localprofile;

  static __thread bool owner;

  static inline void Begin(SpinLock* sl, RTMProfile* prof = NULL)
  {

    int retry = 0;
    int conflict = 0;
    int capacity = 0;
    int zero = 0;
    int nested = 0;
    unsigned stat;

    if(prof )
      prof->start_record();


    while(true) {


      stat = _xbegin();

      if(stat == _XBEGIN_STARTED) {

        if(sl->IsLocked())
          _xabort(0xff);

        return;

      } else {

        if (prof!= NULL) prof->recordAbortStatus(stat);
        retry++;

        if((stat & _XABORT_NESTED) != 0){
          nested++;
        }
        else if(stat == 0)
          zero++;
        else if((stat & _XABORT_CONFLICT) != 0)
          conflict++;
        else if((stat & _XABORT_CAPACITY) != 0)
          capacity++;
        if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
          while(sl->IsLocked())
            _mm_pause();
        }



        int step = 1;
        if (nested > MAXNEST)
        {
          break;
        }

        if(zero > MAXZERO/step) {
          break;
        }

        if(capacity > MAXCAPACITY / step) {
          break;
        }

        if (conflict > MAXCONFLICT/step) {
          break;
        }

      }
    }

    sl->Lock();
    //	owner = true;
    //    if(prof != NULL) {
    //      prof->recordLock();
    //    }
    //    if(prof)
    //      prof->fallback_record();
    //    else
    //      assert(false);

    //	localprofile.recordLock();
    if(prof != NULL)
      prof->recordAbortStatus(stat);
  }


  static inline void End(SpinLock* sl)
  {
    if (_xtest()){
      _xend();
      return;
    }
    if(sl && sl->IsLocked())
      sl->Unlock();
    //		else if(sl->IsLocked() && !owner)
    //		 _xabort(0xff);


  }
  static inline void Begin(SpinLock** sl, int numOfLocks, RTMProfile* prof = NULL)
  {

    int retry = 0;
    int conflict = 0;
    int capacity = 0;
    int zero = 0;
    int nested = 0;
    unsigned stat;
    if(prof )
      prof->start_record();

    while(true) {


      stat = _xbegin();

      if(stat == _XBEGIN_STARTED) {
        for (int i=0; i<numOfLocks; i++)
          if(sl[i]->IsLocked())
            _xabort(0xff);

        return;

      } else {

        if (prof!= NULL) prof->recordAbortStatus(stat);
        retry++;

        if((stat & _XABORT_NESTED) != 0){
          nested++;
        }
        else if(stat == 0)
          zero++;
        else if((stat & _XABORT_CONFLICT) != 0)
          conflict++;
        else if((stat & _XABORT_CAPACITY) != 0)
          capacity++;
        if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
          while(sl[0]->IsLocked())
            _mm_pause();
        }



        int step = 1;
        if (nested > MAXNEST)
        {
          break;
        }

        if(zero > MAXZERO/step) {
          break;
        }

        if(capacity > MAXCAPACITY / step) {
          break;
        }

        if (conflict > MAXCONFLICT/step) {
          break;
        }

      }
    }

    for (int i=0; i<numOfLocks; i++)
      sl[i]->Lock();

    //	owner = true;
    if(prof != NULL) {
      prof->recordLock();
    }
    if(prof)
      prof->fallback_record();

    //	ulocalprofile.recordLock();
    //	localprofile.recordAbortStatus(stat);
  }

  static inline void RdmaBegin(SpinLock *sl,RTMProfile *prof = NULL,bool *abort_flag = NULL) {
    int retry = 0;
    int conflict = 0;
    int capacity = 0;
    int zero = 0;
    int nested = 0;
    unsigned stat;

    if(prof )
      prof->start_record();

    while(true) {


      stat = _xbegin();
      //put the local locks in read-set
      if(stat == _XBEGIN_STARTED) {
        //		    for (int i=0; i<numOfLocks; i++)
        //		      if(sl[i]->IsLocked())
        //			_xabort(0xff);
        if(sl && sl->IsLocked())
          _xabort(0xff);

        return;

      } else {

        if (prof!= NULL) prof->recordAbortStatus(stat);
        retry++;

        if((stat & _XABORT_NESTED) != 0){
          nested++;
        }
        else if(stat == 0)
          zero++;
        else if((stat & _XABORT_CONFLICT) != 0)
          conflict++;
        else if((stat & _XABORT_CAPACITY) != 0)
          capacity++;
        else if((stat & _XABORT_EXPLICIT && _XABORT_CODE(stat) == 0x73)) {
          if(prof != NULL) {
            prof->explicitAbortCounts++;
          }
          break;
        }else if((stat & _XABORT_EXPLICIT && _XABORT_CODE(stat) == 0x93)) {
          *abort_flag = true;
          break;
        }
        if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
          while(sl != NULL && sl->IsLocked())
            _mm_pause();
        }



        int step = 1;
        if (nested > MAXNEST)
        {
          break;
        }

        if(zero > MAXZERO/step) {
          break;
        }

        if(capacity > MAXCAPACITY / step) {
          break;
        }

        if (conflict > MAXCONFLICT/step) {
          break;
        }

      }

    }
    if(sl)
      sl->Lock();
    if(prof)
      prof->fallback_record();
  }

  static inline void RdmaBegin(SpinLock** sl, int numOfLocks, RTMProfile* prof = NULL,bool *abort_flag = NULL)
  {

    int retry = 0;
    int conflict = 0;
    int capacity = 0;
    int zero = 0;
    int nested = 0;
    unsigned stat;

    if(prof )
      prof->start_record();

    while(true) {
      stat = _xbegin();
      //put the local locks in read-set
      if(stat == _XBEGIN_STARTED) {
        for (int i=0; i<numOfLocks; i++)
          if(sl[i]->IsLocked())
            _xabort(0xff);
        return;

      } else {

        if (prof!= NULL) prof->recordAbortStatus(stat);
        retry++;

        if((stat & _XABORT_NESTED) != 0){
          nested++;
        }
        else if(stat == 0)
          zero++;
        else if((stat & _XABORT_CONFLICT) != 0)
          conflict++;
        else if((stat & _XABORT_CAPACITY) != 0)
          capacity++;
        else if((stat & _XABORT_EXPLICIT && _XABORT_CODE(stat) == 0x73)) {
          if(prof != NULL) {
            prof->explicitAbortCounts++;
          }
          break;
        }else if((stat & _XABORT_EXPLICIT && _XABORT_CODE(stat) == 0x93)) {
          *abort_flag = true;
          break;
        }
        if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
          while(sl != NULL && sl[0]->IsLocked())
            _mm_pause();
        }

        int step = 1;
        if (nested > MAXNEST)
        {
          break;
        }

        if(zero > MAXZERO/step) {
          break;
        }

        if(capacity > MAXCAPACITY / step) {
          break;
        }

        if (conflict > MAXCONFLICT/step) {
          break;
        }

      }
    }
    if(prof)
      prof->fallback_record();

    //for (int i=0; i<numOfLocks; i++)
    //sl[i]->Lock();
    //!! Donot need it ,avoid dead lock
    //	owner = true;
    //if(prof != NULL) {
    //prof->recordLock();
  }
  //	localprofile.recordLock();
  //	localprofile.recordAbortStatus(stat);


  static inline void End(SpinLock** sl, int numOfLocks)
  {
    if (_xtest()) {
      _xend();
      return;
    }
    for (int i=0; i< numOfLocks; i++)
      if(sl[i]->IsLocked())
        sl[i]->Unlock();


  }

  static inline void Abort() {
    _xabort(0x1);
  }

  static inline void Abort(SpinLock* sl) {
    if (_xtest()) _xabort(0x1);
    else if(sl->IsLocked())
      sl->Unlock();
  }

  static inline void Begin(SpinLock* sl, SpinLock* s2, RTMProfile* prof = NULL)
  {

    int retry = 0;
    int conflict = 0;
    int capacity = 0;
    int zero = 0;
    int nested = 0;
    unsigned stat;

    while(true) {


      stat = _xbegin();

      if(stat == _XBEGIN_STARTED) {

        if(sl->IsLocked())
          _xabort(0xff);
        else if  (s2->IsLocked())
          _xabort(0xff);
        return;

      } else {

        if (prof!= NULL) prof->recordAbortStatus(stat);
        retry++;

        if((stat & _XABORT_NESTED) != 0){
          nested++;
        }
        else if(stat == 0)
          zero++;
        else if((stat & _XABORT_CONFLICT) != 0)
          conflict++;
        else if((stat & _XABORT_CAPACITY) != 0)
          capacity++;
        if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
          while(sl->IsLocked())
            _mm_pause();
        }



        int step = 1;
        if (nested > MAXNEST)
        {
          break;
        }

        if(zero > MAXZERO/step) {
          break;
        }

        if(capacity > MAXCAPACITY / step) {
          break;
        }

        if (conflict > MAXCONFLICT/step) {
          break;
        }

      }
    }


    sl->Lock();
    s2->Lock();
    //	owner = true;
    if(prof != NULL) {
      prof->recordLock();
    }
    //	localprofile.recordLock();
    //	localprofile.recordAbortStatus(stat);
  }


  static inline void End(SpinLock* sl,SpinLock*s2)
  {
    if (_xtest())
      _xend();

    else {
      sl->Unlock();
      s2->Unlock();
    }
  }



};


//Invoke rtm begin and end explicitly
class RAWRTMTX {

 public:
  static inline void Begin()
  {
    while(true) {

      unsigned stat;
      stat = _xbegin();
      if(stat == _XBEGIN_STARTED) {
        return;
      }
    }
  }


  static inline void End()
  {
    _xend();
  }

  static inline void Abort() {
    _xabort(0x1);
  }

};




#endif  // STORAGE_LEVELDB_UTIL_RTM_H_
