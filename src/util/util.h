#ifndef NOCC_UTIL_H
#define NOCC_UTIL_H

/* This util file is imported from DrTM+ */

#include <sched.h>
#include <time.h>
#include <assert.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>    /* sort  */
#include <climits>

#include <math.h>       /* floor */

#include <unistd.h>

#include <sys/mman.h>

#define ALWAYS_INLINE __attribute__((always_inline))
inline ALWAYS_INLINE uint64_t rdtsc(void) { uint32_t hi, lo; __asm volatile("rdtsc" : "=a"(lo), "=d"(hi)); return ((uint64_t)lo)|(((uint64_t)hi)<<32); }
#define unlikely(x) __builtin_expect(!!(x), 0)
#define MAX(x,y) (  (x) > (y) ? (x) : (y))

namespace nocc {

namespace util {

template <typename R>
constexpr R BitMask(unsigned int const onecount)
{
    return static_cast<R>(-(onecount != 0))
            & (static_cast<R>(-1) >> ((sizeof(R) * CHAR_BIT) - onecount));
}


int BindToCore (int thread_id);
int CorePerSocket();
template <class Num> inline ALWAYS_INLINE  // Round "a" according to "b"
Num Round (Num a, Num b) {
    if(a < b) return b;
    Num r = a % b;
    return r?(a + (b - r)): a ;
}
int  DiffTimespec(const struct timespec &end, const struct timespec &start);

// wrappers for parsing configure file
// !! notice that even a failed parsing will results for cursor advancing
bool NextDouble(std::istream &ist,double &res);
bool NextInt(std::istream &ist,int &res);
bool NextLine(std::istream &ist,std::string &res);
bool NextString(std::istream &ist,std::string &res);
void BypassLine(std::istream &ist);

inline uint64_t TimeToMs(struct timespec &t) { return t.tv_sec * 1000 + t.tv_nsec / 1000000;}

std::pair<uint64_t, uint64_t> get_system_memory_info(); // instruction's memory comsuption

void *malloc_huge_pages(size_t size,uint64_t huge_page_sz,bool flag = true);

inline double get_memory_size_g(uint64_t bytes) { static double G = 1024.0 * 1024 * 1024; return bytes / G; }

void print_stacktrace(FILE *out = stderr, unsigned int max_frames = 63);

} // namespace util
}   // namespace nocc

// other related utils
#include "timer.h"     // timer helper
#endif
