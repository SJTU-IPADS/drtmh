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

#include <iostream>

#include "varint.h"
#include "macros.h"
#include "util.h"

using namespace std;
using namespace nocc::util;

static void
do_test(uint32_t v)
{
  uint8_t buf[5];
  uint8_t *p = &buf[0];
  p = write_uvint32(p, v);
  ALWAYS_ASSERT(size_t(p - &buf[0]) == size_uvint32(v));

  const uint8_t *p0 = &buf[0];
  uint32_t v0 = 0;
  p0 = read_uvint32(p0, &v0);
  ALWAYS_ASSERT(v == v0);
  ALWAYS_ASSERT(p == p0);
}

void
varint::Test()
{
  fast_random r(2043859);
  for (int i = 0; i < 1000; i++)
    do_test(r.next_u32());
  cerr << "varint tests passed" << endl;
}
