#ifndef _ALL_MICRO
#define _ALL_MICRO

#include "rdmaio.h"
#include "db_statistics_helper.h"
#include "../utils/utils.h"
#include "TestConfigXml.h"


#include <sys/mman.h>
#include <sys/time.h>

#include <iostream>
#include <vector>
#include <string>
#include <ctime>

#define MAX_SERVERS 16

using namespace std;

using namespace rdmaio;
using namespace rdmaio::util;


extern TestConfig config;
/* The helper functions needed for microbenchmarks
 * These including:
 * 1) timing utilties
 * 2) core binding
 * 3) fast random
 * 4) huge page allocation
 */

#define CACHELINE_SIZE 64
struct thread_config {
  int node_id;
  int num_threads;
  int id;
  ibv_wr_opcode opcode;
  RdmaCtrl *cm; //communication manager
};

#ifdef __linux
#define SERVER
#endif

#ifdef SERVER

static const int per_socket_cores = 10; //TODO!! hard coded
//int per_socket_cores = 8;//reserve 2 cores

static int socket_0[] = {
  1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47
};

static int socket_1[] = {
  0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46
};


inline void bindToCore(int tid) {
  int y;
  if ( tid >= per_socket_cores) {
    //there is no other cores in the first socket
    y = socket_1[tid - per_socket_cores];
  } else {
    y = socket_0[tid];
  }
  cpu_set_t  mask;
  CPU_ZERO(&mask);
  CPU_SET(y, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);
}

#endif


#endif

#define HUGE_PAGE_SIZE (2 * 1024 * 1024)
inline void* malloc_huge_pages(size_t size,uint64_t huge_page_sz,bool flag) {
  char *ptr; // the return value
#define ALIGN_TO_PAGE_SIZE(x)  (((x) + huge_page_sz - 1) / huge_page_sz * huge_page_sz)
  size_t real_size = ALIGN_TO_PAGE_SIZE(size + huge_page_sz);

  if(flag) {
    // Use 1 extra page to store allocation metadata
    // (libhugetlbfs is more efficient in this regard)
    char *ptr = (char *)mmap(NULL, real_size, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS |
                           MAP_POPULATE | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) {
      // The mmap() call failed. Try to malloc instead
      fprintf(stderr, "[Util] huge page alloced failed...\n");
      goto ALLOC_FAILED;
    } else {
      // Save real_size since mmunmap() requires a size parameter
      *((size_t *)ptr) = real_size;
      // Skip the page with metadata
      return ptr + huge_page_sz;
    }
  }
ALLOC_FAILED:
  assert(false);
  ptr = (char *)malloc(real_size);
  if (ptr == NULL) return NULL;
  real_size = 0;
  return ptr + huge_page_sz;
}


inline vector<Qp *> bootstrap_rc_qps(RdmaCtrl *cm,int tid,int dev_id,int port_idx) {
  vector<Qp *> ret;
  cm->link_connect_qps(tid,dev_id,port_idx,0,IBV_QPT_RC);
  for(uint n_id = 0;n_id < cm->get_num_nodes(); n_id++) {
    ret.push_back(cm->get_rc_qp(tid,n_id));
  }
  
  return ret;
}

inline vector<vector<Qp *>> bootstrap_uc_qps(RdmaCtrl *cm,int tid,int dev_id,int port_idx) {
  vector<vector<Qp *>> ret;
  for(uint n_id = 0; n_id < cm->get_num_nodes(); n_id++) {
    ret.push_back(vector<Qp*>());
  }
  for(uint idx = 0; idx < config.uc_per_thread_; idx++){
    cm->link_connect_qps(tid,dev_id,port_idx,idx,IBV_QPT_UC);
    for(uint n_id = 0;n_id < cm->get_num_nodes(); n_id++) {
      ret[n_id].push_back(cm->get_uc_qp(tid,n_id,idx));
    } 
  }
  return ret;
}

inline vector<Qp *> bootstrap_thread_qps(RdmaCtrl *cm,int tid,int dev_id,int port_idx) {

  // check the time
  {
    char buffer[80];

    time_t rawtime;
    time (&rawtime);

    tm *timeinfo = localtime(&rawtime);

    strftime(buffer,sizeof(buffer),"%d-%m-%Y %I:%M:%S",timeinfo);
    string str(buffer);
    cout<<str<<endl;
  }

  // uint64_t begin = rdtsc();
  // clock_t start = clock();
  // Timer t;
  // if(config.rc_per_thread_ > 0 || config.uc_per_thread_ > 0 || config.ud_per_thread_){
    
  // }
  // if(config.ud_per_thread_ > 0){
  //   cm->register_dgram_mr(dev_id);
  // }
  // uint64_t gap = rdtsc() - begin;
  // t.end();
  // fprintf(stdout,"takes %f seconds to register mem.\n", t.elapsed_sec());


  // create qps
  vector<Qp *> ret;

  if(config.rc_per_thread_ > 0) {
    cm->link_connect_qps(tid,dev_id,port_idx,0,IBV_QPT_RC);
    for(uint i = 0;i < cm->get_num_nodes();++i) {
      ret.push_back(cm->get_rc_qp(tid,i));
    } 
    // fprintf(stdout,"takes %f seconds to create rc qps.\n",t.elapsed_sec());
  } else if(config.uc_per_thread_ > 0) {
    cm->link_connect_qps(tid,dev_id,port_idx,0,IBV_QPT_UC);
    for(uint i = 0;i < cm->get_num_nodes();++i) {
      ret.push_back(cm->get_uc_qp(tid,i));
    } 
    // fprintf(stdout,"takes %f seconds to create uc qps.\n",t.elapsed_sec());
  } else if(config.ud_per_thread_ > 0) {
    Qp *send_qp = cm->create_ud_qp(tid,dev_id,port_idx,0);
    Qp *recv_qp = cm->create_ud_qp(tid,dev_id,port_idx,1);

    while(1) {
      int connected = 0;
      for(uint i = 0;i < cm->get_num_nodes();++i) {
        if(send_qp->get_ud_connect_info(i,1))
          connected += 1;
      }
      if(connected == cm->get_num_nodes())
        break;
    }
    // fprintf(stdout,"takes %f seconds to create ud qps.\n",t.elapsed_sec());
  }
  return ret;
}

/* The following fast_random utilites are from Silo */

class fast_random {
public:
  fast_random(unsigned long seed): seed(0) {
    set_seed0(seed);
  }

  inline unsigned long next() {
    return ((unsigned long) next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() {
    return next(32);
  }

  inline uint16_t next_u16() {
    return next(16);
  }

  /** [0.0, 1.0) */
  inline double next_uniform() {
    return (((unsigned long) next(26) << 27) + next(27)) / (double) (1L << 53);
  }

  inline char next_char() {
    return next(8) % 256;
  }

  inline std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_char();
    return s;
  }

  inline unsigned long get_seed() {
  return seed;
  }

  inline void set_seed(unsigned long seed) {
    this->seed = seed;
  }

  private:
  inline void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long) (seed >> (48 - bits));
  }

  unsigned long seed;
};
