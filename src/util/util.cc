#include "util.h"

#include <sys/sysinfo.h>
#include <utility>
#include <assert.h>

#include <execinfo.h>
#include <cxxabi.h>

//#define HYPER
#ifdef  HYPER // hyper threading

static const int per_socket_cores = 24;//TODO!! hard coded

static int socket_0[] =  {
  0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46
};

static int socket_1[] = {
  1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47
};


#else

//methods for set affinity
static const int per_socket_cores = 12;//TODO!! hard coded
//const int per_socket_cores = 8;//reserve 2 cores

static int socket_0[] =  {
  0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46
};

static int socket_1[] = {
  1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47
};

#endif

namespace nocc {

namespace util {

int CorePerSocket() {
  return per_socket_cores;
}

//Note! This impls only work for vals
int BindToCore(int t_id) {

  if(t_id >= (per_socket_cores * 2))
    return 0;

  int x = t_id;
  int y = 0;

#ifdef SCALE
  assert(false);
  //specific  binding for scale tests
  int mac_per_node = 16 / nthreads;//there are total 16 threads avialable
  int mac_num = current_partition % mac_per_node;

  if (mac_num < mac_per_node / 2) {
    y = socket_0[x + mac_num * nthreads];
  }else {
    y = socket_1[x + (mac_num - mac_per_node / 2) * nthreads];
  }
#else
  //bind ,andway
  if ( x >= per_socket_cores ) {
    //there is no other cores in the first socket
    y = socket_1[x - per_socket_cores];
  }else {
    y = socket_0[x];
  }

#endif

  //fprintf(stdout,"worker: %d binding %d\n",x,y);
  cpu_set_t  mask;
  CPU_ZERO(&mask);
  CPU_SET(y , &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  return 0;
}


int
DiffTimespec(const struct timespec &end, const struct timespec &start)
{
  // FIXME!!
  // ma,there is a 2038 problem, but it's 2016 now 2333
  int diff = (end.tv_sec > start.tv_sec)?(end.tv_sec-start.tv_sec)*1000:0;
  assert(diff || end.tv_sec == start.tv_sec);
  if (end.tv_nsec > start.tv_nsec) {
    diff += (end.tv_nsec-start.tv_nsec)/1000000;
  } else {

    diff -= (start.tv_nsec-end.tv_nsec)/1000000;
  }
  return diff;
}


/* */
//simple functions for parsing configuration file
//we do not care performance

bool NextDouble(std::istream &ist,double &res) {
  ist >> std::ws;
  char s = ist.peek();
  if( !(s >= '0' && s <= '9')) { return false; }
  bool ret =  static_cast<bool>(ist >> res);
  return ret;
}

bool NextInt(std::istream &ist,int &res) {

  ist >> std::ws;
  char s = ist.peek();
  if( !(s >= '0' && s <= '9')) { return false; }
  return static_cast<bool>(ist >> res);
}

bool NextLine(std::istream &ist,std::string &res) {
  return static_cast<bool>(std::getline(ist,res));
}

bool NextString(std::istream &ist,std::string &res) {
  return static_cast<bool>(ist >> res);
}


void BypassLine(std::istream &ist) {
  std::string temp;
  std::getline(ist,temp);
}


std::pair<uint64_t, uint64_t> get_system_memory_info()
{
  struct sysinfo inf;
  sysinfo(&inf);
  return std::make_pair(inf.mem_unit * inf.freeram, inf.mem_unit * inf.totalram);
}

void *malloc_huge_pages(size_t size,uint64_t huge_page_sz,bool flag)
{
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
      fprintf(stdout,"[Util] Huge page alloc real size %fG\n",get_memory_size_g(real_size));
      // Save real_size since mmunmap() requires a size parameter
      *((size_t *)ptr) = real_size;
      // Skip the page with metadata
      return ptr + huge_page_sz;
    }
  }
ALLOC_FAILED:
  ptr = (char *)malloc(real_size);
  if (ptr == NULL) return NULL;
  real_size = 0;
  return ptr + huge_page_sz;
}

// Credits: This lovely function is from **https://panthema.net/2008/0901-stacktrace-demangled/**
void print_stacktrace(FILE *out, unsigned int max_frames)
{
  fprintf(out, "stack trace:\n");

  // storage array for stack trace address data
  void* addrlist[max_frames+1];

  // retrieve current stack addresses
  int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

  if (addrlen == 0) {
    fprintf(out, "  <empty, possibly corrupt>\n");
    return;
  }

  // resolve addresses into strings containing "filename(function+address)",
  // this array must be free()-ed
  char** symbollist = backtrace_symbols(addrlist, addrlen);

  // allocate string which will be filled with the demangled function name
  size_t funcnamesize = 256;
  char* funcname = (char*)malloc(funcnamesize);

  // iterate over the returned symbol lines. skip the first, it is the
  // address of this function.
  for (int i = 1; i < addrlen; i++)
  {
    char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

    // find parentheses and +address offset surrounding the mangled name:
    // ./module(function+0x15c) [0x8048a6d]
    for (char *p = symbollist[i]; *p; ++p)
    {
      if (*p == '(')
        begin_name = p;
      else if (*p == '+')
        begin_offset = p;
      else if (*p == ')' && begin_offset) {
        end_offset = p;
        break;
      }
    }

    if (begin_name && begin_offset && end_offset
        && begin_name < begin_offset)
    {
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      // mangled name is now in [begin_name, begin_offset) and caller
      // offset in [begin_offset, end_offset). now apply
      // __cxa_demangle():

      int status;
      char* ret = abi::__cxa_demangle(begin_name,
                                      funcname, &funcnamesize, &status);
      if (status == 0) {
        funcname = ret; // use possibly realloc()-ed string
        fprintf(out, "  %s : %s+%s\n",
                symbollist[i], funcname, begin_offset);
      }
      else {
        // demangling failed. Output function name as a C function with
        // no arguments.
        fprintf(out, "  %s : %s()+%s\n",
                symbollist[i], begin_name, begin_offset);
      }
    }
    else
    {
      // couldn't parse the line? print the whole line.
      fprintf(out, "  %s\n", symbollist[i]);
    }
  }

  free(funcname);
  free(symbollist);
}
} // namespace util
}   // namespace nocc
