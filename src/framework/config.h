#ifndef NOCC_FRAMEWORK_CONFIG_H_
#define NOCC_FRAMEWORK_CONFIG_H_

#define MASTER_EPOCH 15 // runtime of test

// rdma related stuffs
#define HUGE_PAGE  1
#define USE_UD_MSG 1
#define USE_TCP_MSG 0
#define SINGLE_MR  0
#define BUF_SIZE   10480 // RDMA buffer size registered, in a small setting
//#define BUF_SIZE 512

// rpc related stuffs
//#define RPC_TIMEOUT_FLAG
#define RPC_TIMEOUT_TIME 10000000
//#define RPC_CHECKSUM
#define MAX_INFLIGHT_REPLY 256
//#define MAX_INFLIGHT_REQS  768  // for TPC-C, smallbank usage
#define MAX_INFLIGHT_REQS 128
//#define MAX_INFLIGHT_REQS 16


// print statements
#define LOG_RESULTS           // log results to a file
#define LISTENER_PRINT_PERF 1 // print the results to the screen
#define PER_THREAD_LOG 0      // per thread detailed log
#define POLL_CYCLES    0      // print the polling cycle
#define CALCULATE_LAT  1
#define LATENCY 0 // 1: global latency report; 0: workload specific report

#if USE_TCP_MSG == 1
#undef  USE_UD_MSG
#define USE_UD_MSG 0
#endif



#endif
