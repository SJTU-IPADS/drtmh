/*
 * This file implements a timestamp manager for SI
 * The timestamp is distributed accoring to the paper "The End of a Myth: Distributed Transactions Can Scale".
 *
 */

#ifndef NOCC_TX_SI_TS_
#define NOCC_TX_SI_TS_

#include <functional>
#include <vector>
#include <stdint.h>

#include "rdmaio.h"
#include "all.h"

typedef std::function<void *(void *)> ts_manage_func_t;

#define LARGE_VEC 0 // whether uses a per-worker vector timestamp

extern size_t nthreads;

namespace nocc {
  namespace db {

    /* timestamp structure
       | 1 lock bit | 15 bit worker id | worker's local counter |
    */
#define SI_TS_MASK  (0xffffffffffff)
#define SI_SERVER_MASK (0xffff)

#define SI_GET_SERVER(x)   (((x) >> 48) & SI_SERVER_MASK)
#define SI_GET_COUNTER(x)  ((x) & SI_TS_MASK)
#define SI_ENCODE_TS(s,t)  (((s) << 48) | (t))

    /* there would be exactly one of this class */
    class TSManager {
    public:
#if LARGE_VEC == 1
      static __thread uint64_t local_timestamp_;
#endif
      TSManager(rdmaio::RdmaCtrl *q,uint64_t time_addr,int id,int master_id,int worker_id );

      /* The timestamp is a vector timestamp, one per server */
      void get_timestamp(char *buffer,int tid);

      /* Ts manaer thread is used to update the true commit timestamp */
      void *ts_monitor(void *);
      void *timestamp_poller(void *);
      void thread_local_init();

      inline uint64_t get_commit_ts() {
        return __sync_fetch_and_add(&local_timestamp_,1);
      }

      static void print_ts(char *ts_ptr,int num) {
        static uint64_t tv_size = num;
        for(uint printed = 0; printed < sizeof(uint64_t) * tv_size; printed += sizeof(uint64_t)) {
          fprintf(stdout,"%lu\t",*((uint64_t *)(ts_ptr + printed)));
        }
        fprintf(stdout,"\n");
      }

      static void initilize_meta_data(char *buffer,int partitions) {
        uint64_t *arr = (uint64_t *)buffer;
        int arr_entries = partitions;
#if LARGE_VEC == 1
        // ensure that the total size of timestamp is smaller than the meta data size
        assert(partitions * nthreads * sizeof(uint64_t)  < 2 * 1024 * 1024);
        arr_entries = arr_entries * nthreads;
#else
#endif
        for(uint i = 0;i < arr_entries;++i) {
          /*FIXME i know 2 is a magic number. The existence is due to a historical reason */
          arr[i] = 2;
        }
      }

      rdmaio::RdmaCtrl *cm_;
      /* the master server which stores the timestamp */
      int master_id_;
      uint64_t ts_addr_;
      int      total_partition;
      int tv_size_;              // total size of the timestamp vector
#if LARGE_VEC == 0
      uint64_t local_timestamp_;
      uint64_t last_ts_;
#endif
    private:
      int worker_id_;            // worker id
      char *fetched_ts_buffer_;  // an offset of where to read for the timestamp
      int id_;                   // current server's id
    } __attribute__ ((aligned(CACHE_LINE_SZ))) ;

  };
};
#endif
