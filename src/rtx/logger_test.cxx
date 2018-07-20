#include <iostream>

#include "gtest/gtest.h"
#include "log_mem_manager.hpp"
#include "log_store_manager.hpp"

#include "core/utils/util.h"

using namespace nocc::rtx;
using namespace nocc::util;

int total_mac = 12;
int total_thread = 12;

// First is test hierachy, second is the specific test namespace
TEST(logger_test, log_mem_offset_case)
{

  char *test_ptr = (char *)malloc(1024 * 1024 * 12);
  auto test_manager = LogMemManager(test_ptr,total_mac,total_thread,4096);
  LogStoreManager manager(2);
  // test thread id
  for(uint tid = 0;tid < total_thread;++tid) {
    for(uint fmid = 0;fmid < total_mac;++fmid) {
      for(uint ttid = 0;ttid < total_mac;++ttid) {
        EXPECT_EQ(test_manager.get_remote_log_offset(fmid,tid,ttid,0) + test_ptr,
                  test_manager.get_local_log_ptr(fmid,tid));
      }
    }
  }
  //
  free(test_ptr);
}

TEST(logger_test,send_recv_case) {
  char *test_buffer = (char *)malloc(1024 * 1024 * 12);

  fast_random rand(73);

  // 1024 test case
  for(uint tid = 0;tid < total_thread;++tid) {

    auto test_manager = LogMemManager(test_buffer,total_mac,total_thread,4096);

    for(uint i = 0;i < 1024;++i) {
      int send_mac = rand.next() % total_mac;
      int to_mac   = rand.next() % total_mac;

      int send_tid = tid;
      int send_value = rand.next();

      int log_size = rand.next() % 128;
      log_size = log_size + 64 - log_size % 64; // aligment

      char *send_ptr = test_manager.get_remote_log_offset(send_mac,tid,send_mac,log_size) + test_buffer;
      *(uint64_t *)send_ptr = send_value;

      const char *recv_ptr = test_manager.get_next_log(send_mac,tid,log_size);
      EXPECT_EQ(recv_ptr,send_ptr);
      ASSERT_EQ((*(uint64_t *)recv_ptr),send_value);
    }
  }
  delete test_buffer;
}
