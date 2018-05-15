// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/spinlock.h"


namespace leveldb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex *mu) EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu)  {
    this->mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

 private:
  port::Mutex *const mu_;
  // No copying allowed
  MutexLock(const MutexLock&);
  void operator=(const MutexLock&);
};


class SCOPED_LOCKABLE MutexSpinLock {
 public:
  explicit MutexSpinLock(port::SpinLock *mu) EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu)  {
    this->mu_->Lock();
  }
  ~MutexSpinLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

 private:
  port::SpinLock *const mu_;
  // No copying allowed
  MutexSpinLock(const MutexSpinLock&);
  void operator=(const MutexSpinLock&);
};

class SCOPED_LOCKABLE SpinLockScope {
 public:
  explicit SpinLockScope(SpinLock *slock)
      : slock_(slock)  {
    slock_->Lock();
  }
  ~SpinLockScope() { slock_->Unlock(); }

 private:
  SpinLock *slock_;
  // No copying allowed
  SpinLockScope(const SpinLockScope&);
  void operator=(const SpinLockScope&);
};


}  // namespace leveldb


#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
