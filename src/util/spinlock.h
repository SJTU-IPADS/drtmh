#ifndef LEVELDB_SPINLOCK_H
#define LEVELDB_SPINLOCK_H

#include <stdint.h>
#include "port/atomic.h"

/* The counter should be initialized to be 0. */
class SpinLock  {

public:
  //0: free, 1: busy
  //occupy an exclusive cache line
  volatile uint8_t padding1[32];
  volatile uint16_t lock;
  volatile uint8_t padding2[32];
public:

  SpinLock(){ lock = 0;}
  
  inline void Lock() {
    while (1) {
       if (!xchg16((uint16_t *)&lock, 1)) return;
   
       while (lock) cpu_relax();
   }
  }

  inline void Unlock() 
  {
  	  barrier();
      lock = 0;
  }


  inline uint16_t Trylock()
  {
  	return xchg16((uint16_t *)&lock, 1);
  }

  inline uint16_t IsLocked(){return lock;}


};

#endif /* _RWLOCK_H */
