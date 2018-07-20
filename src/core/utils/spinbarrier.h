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

#ifndef _SPINBARRIER_H_
#define _SPINBARRIER_H_

#include "amd64.h"
#include "macros.h"
#include "util.h"

/**
 * Barrier implemented by spinning
 */

class spin_barrier {
public:
  spin_barrier(size_t n)
    : n(n)
  {
    ALWAYS_ASSERT(n > 0);
  }
#if 0
  spin_barrier(const spin_barrier &) = delete;
  spin_barrier(spin_barrier &&) = delete;
  spin_barrier &operator=(const spin_barrier &) = delete;
#endif
  ~spin_barrier()
  {
    //ALWAYS_ASSERT(n == 0);
  }

  void
  count_down()
  {
    // written like this (instead of using __sync_fetch_and_add())
    // so we can have assertions
    for (;;) {
      size_t copy = n;
      ALWAYS_ASSERT(copy > 0);
      if (__sync_bool_compare_and_swap(&n, copy, copy - 1))
        return;
    }
  }

  void
  wait_for()
  {
    while (n > 0)
      nop_pause();
  }

 public:
  volatile size_t n;
};

#endif /* _SPINBARRIER_H_ */
