/* This file contains constants used in the framework. ************************/

#ifndef _ALL
#define _ALL

/* Coroutine related staff */
/* Using boost coroutine   */
#include<boost/coroutine/all.hpp>
#include "util/printer.h"

typedef boost::coroutines::symmetric_coroutine<void>::call_type coroutine_func_t;
typedef boost::coroutines::symmetric_coroutine<void>::yield_type yield_func_t;

namespace nocc {

/***** hardware parameters ********/
#define CACHE_LINE_SZ 64                // cacheline size of x86 platform
//#define MAX_MSG_SIZE  25600              // max msg size used
#define MAX_MSG_SIZE 4096

#define HUGE_PAGE_SZ (2 * 1024 * 1024)  // huge page size supported

#define MAX_SERVERS 32                  // maxium number of machines in the cluster
#define MAX_SERVER_TO_SENT MAX_SERVERS  // maxium broadcast number of one msg

#define NOCC_BENCH_MAX_TX 16

/**********************************/


/* some usefull macros  ******************************************************/
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

#define NOCC_NOT_IMPLEMENT(fnc)  { fprintf(stderr,"[NOCC] %s function not implemented.\n",fnc);assert(false); }
}

#endif
