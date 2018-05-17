#pragma once

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
    };



  } // namespace db
};  // namespace nocc
