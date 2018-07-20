#pragma once

#include <map>
#include "memstore/memdb.h"

namespace nocc {

  namespace db {

    class LogHelper {

      static int mac_num;
      static int threads;
      static int coroutine_num;

      static int mac_log_area;
      static int thread_log_area;

      static char *log_base_ptr;

      static int max_log_size;

    public:

      // base ptr, total partitions, total threads, total coroutines
      static void configure(char *ptr, int ms,int ts,int cs,int log_size) {

        max_log_size = log_size;

        mac_num = ms;
        threads = ts;
        coroutine_num = cs;

        mac_log_area    = coroutine_num * max_log_size;
        thread_log_area = mac_num * mac_log_area;

        log_base_ptr = ptr;

      }

      static uint64_t get_log_size() {
        return thread_log_area * threads;
      }


      static char *get_thread_ptr(int tid) {
        return log_base_ptr + tid * thread_log_area;
      }

      inline static int get_off(int mid,int cid) {
        return mid * (coroutine_num * max_log_size) + cid * max_log_size;
      }

      LogHelper() {

      }

      void add_backup_store(int id,MemDB *backup_store){
        backup_stores_.insert(std::make_pair(id,backup_store));
      }

      inline MemDB *get_backed_store(int id) {
        assert(backup_stores_.size() > 0);
        if(backup_stores_.find(id) == backup_stores_.end()) {
          fprintf(stdout,"error %d\n",id);
          assert(false);
        }
        return backup_stores_[id];
      }

    private:
      std::unordered_map<int,MemDB*> backup_stores_;
    };



  } // namespace db
};  // namespace nocc
