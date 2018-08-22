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

#ifndef _NDB_THREAD_H_
#define _NDB_THREAD_H_

#include <pthread.h>
#include <vector>
#include <string>

#include "macros.h"
#include "util.h"

/**
 * Threads in NuDB
 *
 * Threads in NuDB should never be explicitly created with
 * pthread_create()! This allows us to wrap threads with handlers on
 * thread completion
 *
 * Note that ndb_threads are thin wrappers around pthread's API
 */

class ndb_thread {
public:

  typedef void (*run_t)(void);

  ndb_thread(bool daemon = false, const std::string &name = "thd")
    : body(NULL), daemon(daemon), name(name) {}
  ndb_thread(run_t body, bool daemon = false, const std::string &name = "thd")
    : body(body), daemon(daemon), name(name) {}

  ndb_thread(const ndb_thread &) = delete;
  ndb_thread(ndb_thread &&) = delete;
  ndb_thread &operator=(const ndb_thread &) = delete;

  virtual ~ndb_thread();

  inline pthread_t
  pthread_id() const
  {
    return p;
  }

  inline const std::string &
  get_name() const
  {
    return name;
  }

  void start();

  void join();

  void binding(int id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(2 * id, &cpuset);
    int rc = pthread_setaffinity_np(p,
                                    sizeof(cpu_set_t), &cpuset);
    assert(rc == 0);
  }
  virtual void run() = 0;

  virtual void set_local_worker() {

  }

  typedef void (*callback_t)(ndb_thread *);

  /**
   * callback registration is assumed to happen at static initialization time,
   * and thus be single-threaded
   */
  static bool register_completion_callback(callback_t callback);

private:
  pthread_t p;
  run_t body;
  const bool daemon;
  const std::string name;

  static std::vector<callback_t> &completion_callbacks();

  void on_complete();
  static void *pthread_bootstrap(void *p);
};

#define NDB_THREAD_REGISTER_COMPLETION_CALLBACK(fn) \
  static bool _ndb_thread_callback_register_ ## __LINE__ UNUSED = \
    ::ndb_thread::register_completion_callback(fn);

#endif /* _NDB_THREAD_H_ */
