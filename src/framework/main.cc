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

#include <sys/resource.h>
#include <errno.h>

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>
#include <set>
#include <iterator>

#include <stdio.h>
#include <execinfo.h>
#include <signal.h>

#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>

#include "app/graph/graph.h"
#include "app/tpcc/tpcc_worker.h"
#include "app/tpce/tpce_worker.h"
#include "app/smallbank/bank_worker.h"
#include "app/micro_benches/bench_micro.h"

#include "util/spinlock.h"
#include "util/util.h"

#include "bench_runner.h"

using namespace std;
using namespace nocc::util;

// benchmark parameters
extern size_t coroutine_num;
extern size_t nthreads;
extern size_t nclients;
extern size_t distributed_ratio;
extern size_t scale_factor;

// For distributed use
extern size_t total_partition;
extern size_t current_partition;
extern std::string config_file;

std::string exe_name;       // the executable's name
string bench_type = "tpcc"; // app name

int verbose = 0;
uint64_t txn_flags = 0;
uint64_t ops_per_worker = 0;
int enable_parallel_loading = false;
int pin_cpus = 0;
int slow_exit = 0;
int retry_aborted_transaction = 0;
int no_reset_counters = 0;
int backoff_aborted_transaction = 0;

static SpinLock exit_lock;

namespace nocc {

  extern volatile bool running; // running config variables

  // some helper functions
  static void printTraceExit(int sig) {
    nocc::util::print_stacktrace();
    running = false;
  }

  static void segfault_handler(int sig) {
    exit_lock.Lock();
    fprintf(stdout,"[NOCC] Meet a segmentation fault!\n");
    printTraceExit(sig);
    running = false;
  }


  static void segabort_handler(int sig) {
    exit_lock.Lock();
    fprintf(stdout,"[NOCC] Meet an assertion failure!\n");
    printTraceExit(sig);
    exit(-1);
  }

  static vector<string>
  split_ws(const string &s)
  {
    vector<string> r;
    istringstream iss(s);
    copy(istream_iterator<string>(iss),
         istream_iterator<string>(),
         back_inserter<vector<string>>(r));
    return r;
  }

} // namespace nocc
int  main(int argc, char **argv)
{

  exe_name = std::string(argv[0] + 2);
  void (*test_fn)( int argc, char **argv) = NULL;

  // setting the ulimit
  struct rlimit limit;
  limit.rlim_cur = 2048;
  limit.rlim_max = 2048;

  if (setrlimit( RLIMIT_NOFILE, &limit) != 0) {
    printf("setrlimit() failed with errno=%d\n", errno);
    return 1;
  }

  char time_buffer[80];
  {
    // calculate the run time for debug
    time_t rawtime;
    struct tm * timeinfo;

    time (&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(time_buffer,sizeof(time_buffer),"%d-%m-%Y %I:%M:%S",timeinfo);
  }
  fprintf(stdout,"NOCC started with program [%s]. at %s\n",exe_name.c_str(),time_buffer);

  char  *curdir = get_current_dir_name();
  string basedir = curdir;
  string bench_opts;
  size_t numa_memory = 0;
  free(curdir);
  int saw_run_spec = 0;
  int nofsync = 0;
  int do_compress = 0;
  int fake_writes = 0;
  int disable_gc = 0;
  int disable_snapshots = 0;
  vector<string> logfiles;
  vector<vector<unsigned>> assignments;
  string stats_server_sockfile;
  while (1) {
    static struct option long_options[] =
      {
        {"verbose"                    , no_argument       , &verbose                   , 1}   ,
        {"parallel-loading"           , no_argument       , &enable_parallel_loading   , 1}   ,
        {"pin-cpus"                   , no_argument       , &pin_cpus                  , 1}   ,
        {"slow-exit"                  , no_argument       , &slow_exit                 , 1}   ,
        {"retry-aborted-transactions" , no_argument       , &retry_aborted_transaction , 1}   ,
        {"backoff-aborted-transactions" , no_argument     , &backoff_aborted_transaction , 1} ,
        {"bench"                      , required_argument , 0                          , 'b'} ,
        {"basedir"                    , required_argument , 0                          , 'B'} ,
        {"txn-flags"                  , required_argument , 0                          , 'f'} ,
        {"ratio"                      , required_argument , 0                          , 'r'} ,
        {"ops-per-worker"             , required_argument , 0                          , 'n'} ,
        {"bench-opts"                 , required_argument , 0                          , 'o'} ,
        {"numa-memory"                , required_argument , 0                          , 'm'} , // implies --pin-cpus
        {"nthreads"                   , required_argument , 0                          , 't'} ,
        {"routines"                   , required_argument , 0                          , 'a'} ,
        {"scale_factor"               , required_argument , 0                          , 's'} ,
        {"total-partition"            , required_argument , 0                          , 'p'} ,
        {"id"                         , required_argument , 0                          , 'i'} ,
        {"config"                     , required_argument , 0                          , 'k'} ,
        {"disable-gc"                 , no_argument       , &disable_gc                , 1}   ,
        {"nclients"      , required_argument , 0                          , 'x'} ,
        {"no-reset-counters"          , no_argument       , &no_reset_counters         , 1}   ,
        {0, 0, 0, 0}
      };
    int option_index = 0;
    int c = getopt_long(argc, argv, "b:s:t:d:B:f:r:n:o:m:l:a:p:c:x:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;
    case 'r':
      distributed_ratio = strtoul(optarg,NULL,10);
      break;
    case 'p':
      total_partition = strtoul(optarg, NULL, 10);
      break;

    case 'i':
      current_partition = strtoul(optarg, NULL, 10);
      break;
    case 'w':
      nclients = strtoul(optarg,NULL,10);
      break;
    case 'c':
      coroutine_num = strtoul(optarg,NULL,10);
      break;

    case 'k':
      nocc::oltp::config_file_name = std::string(optarg);
      break;
    case 'b':
      bench_type = optarg;
      break;

    case 's':
      scale_factor = strtoul(optarg, NULL,10);
      break;

    case 'B':
      basedir = optarg;
      break;

    case 'f':
      txn_flags = strtoul(optarg, NULL, 10);
      break;

    case 'o':
      bench_opts = optarg;
      break;
    case 'm':
      {
        pin_cpus = 1;
        //        const size_t m = parse_memory_spec(optarg);
        //        ALWAYS_ASSERT(m > 0);
        //        numa_memory = m;
      }
      break;
    case 't':
      //      logfiles.emplace_back(optarg);
      nthreads = strtoul(optarg,NULL,10);
      break;

    case 'a':
      //      assignments.emplace_back(
      //          ParseCSVString<unsigned, RangeAwareParser<unsigned>>(optarg));
      coroutine_num = strtoul(optarg,NULL,10);
      break;

    case 'x':
      nclients = strtoul(optarg,NULL,10);
      break;

    case '?':
      /* getopt_long already printed an error message. */
      exit(1);

    default:
      fprintf(stdout,"Invalid command line val: %d\n",c);
      abort();
    }
  }

  if(bench_type == "tpcc") {
    test_fn = nocc::oltp::tpcc::TpccTest;
    //    test_fn = nocc_tpce_benchmark::TpceTest;
    //test_fn = occtpcc_do_test;
#ifndef BASE_LINE
    //test_fn = tpcc_do_test;
#else
    //test_fn = occtpcc_do_test;
#endif
  } else if(bench_type == "tpce") {
    fprintf(stdout,"using benchmark tpce\n");
    //test_fn = nocc::oltp::tpce::TpceTest;
  } else if(bench_type == "micro") {
    fprintf(stdout,"using benchmark micro\n");
    test_fn = nocc::oltp::micro::MicroTest;
  } else if(bench_type == "bank") {
    test_fn = nocc::oltp::bank::BankTest;
  } else if(bench_type == "graph") {
    test_fn = nocc::oltp::link::GraphTest;
  } else{
    ALWAYS_ASSERT(false);
  }
  //#endif

  vector<string> bench_toks = nocc::split_ws(bench_opts);
  int argc = 1 + bench_toks.size();
  char *argv[argc];
  argv[0] = (char *) bench_type.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    argv[i] = (char *) bench_toks[i - 1].c_str();

  /* install the event handler if necessary */
  signal(SIGSEGV, nocc::segfault_handler);
  signal(SIGABRT, nocc::segabort_handler);

  test_fn(argc, argv);
  return 0;
}
