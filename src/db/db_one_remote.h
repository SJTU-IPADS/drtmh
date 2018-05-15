#ifndef _TX_ONE_REMOTE
#define _TX_ONE_REMOTE

// This file implements remote fetch operations using one-sided operator

#include "global_config.h"
#include "core/rdma_sched.h"

#include "memstore/memdb.h" // for db handler

#include "rdmaio.h"         // for rdma operators
#include "db_statistics_helper.h"

#include <vector>

namespace nocc {

  using namespace oltp;
  namespace db {

    class RRWSet {

    public:
      struct RemoteSetItem {
        int   pid;
        int   len;
        int   tableid;

        uint64_t key;
        uint64_t off;
        uint64_t seq;
        char *val;

        bool ro;
      };


      struct Meta { // meta data format
        uint64_t lock;
        uint64_t seq;
      };

      RRWSet(rdmaio::RdmaCtrl *cm,RDMA_sched *sched,MemDB *db,int tid,int cid,int meta);

      int  add(int pid,int tableid,uint64_t key,int len); // fetch the remote key to the cache
      // the data off is written after meta off has been written
      int  add(int pid,int tableid,uint64_t key,int len, yield_func_t &yield);

      bool validate_reads(yield_func_t &yield);
      bool lock_remote(uint64_t lock_content, yield_func_t &yield);
      int  write_all_back(int meta_off,int data_off,char *buffer);
      void release_remote();

      // len does not include the meta data of the record
      inline void clear() { elems_ = 0; read_num_ = 0; }
      inline int  numItems() { return elems_; }
      inline void promote_to_write(int idx) { kvs_[idx].ro = false;read_num_ -= 1; }

      void report() { REPORT(post); }

      RemoteSetItem *kvs_;

    private:
      int elems_;
      int read_num_;

      RDMA_sched *sched_;

      rdmaio::Qp* get_qp(int pid){
        int idx = (qp_idx_[pid]++) % QP_NUMS;
        return qps_[pid*QP_NUMS + idx];
      }

      int qp_idx_[16];
      std::vector<rdmaio::Qp *> qps_; // Qp in-used for one-sided operations
      MemDB    *db_;
      int       tid_; // thread id
      int       cor_id_; // coroutine id
      int       meta_len_;

      LAT_VARS(post);
    };

  }; // namespace db
};   // namespace nocc

#endif
