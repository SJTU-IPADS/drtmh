#pragma once

#include <map>

#include "core/logging.h"

namespace nocc {

namespace rtx {

// The default size of each log entry
#define RTX_LOG_ENTRY_SIZE 2048

class LogMemManager {
 public:
  LogMemManager(char *local_p,int ms,int ts,int size,int entry_size = RTX_LOG_ENTRY_SIZE,uint64_t base_off = 0) :
      mac_num_(ms),
      thread_num_(ts),
      local_buffer_(local_p),
      log_entry_size_(RTX_LOG_ENTRY_SIZE),
      thread_buf_size_(size + RTX_LOG_ENTRY_SIZE),
      base_offset_(base_off)
  {
    //LOG(3) << "add " << ms << " " << ts << " " << size;
    assert(log_entry_size_ <= size);

    total_mac_log_size_ = thread_num_ * thread_buf_size_;

    remote_tailers_ = new uint64_t[mac_num_];
    local_headers_  = new uint64_t[mac_num_];

    for(uint i = 0;i < mac_num_;++i) {
      remote_tailers_[i] = 0;
      local_headers_[i]  = 0;
    }
  }

  ~LogMemManager() {
    delete remote_tailers_;
    delete local_headers_;
  }

  inline uint64_t total_log_size() {
    return total_mac_log_size_ * mac_num_;
  }


  inline uint64_t get_remote_log_offset(int from_mac,int from_tid,int to_mid,int log_size) {
    uint64_t base_offset = from_tid * thread_buf_size_ + from_mac * total_mac_log_size_ +
                           (remote_tailers_[to_mid] % (thread_buf_size_ - log_entry_size_))
                           + base_offset_; // the start pointer of log area
    remote_tailers_[to_mid] += log_size;   // increment the ring buffer pointer
    ///LOG(2) << "tail num " << (thread_buf_size_ - log_entry_size_) <<
    //"thread " << thread_buf_size_ << " log entry " << log_entry_size_;
    //LOG(2) << base_offset;sleep(1);
    return base_offset;
  }

  inline uint64_t get_previous_remote_offset(int from_mac,int from_tid,int to_mid,int log_size) {
    ASSERT(remote_tailers_[to_mid] >= log_size) << "tail " << (uint64_t)(remote_tailers_[to_mid])
                                                << ";log size " << log_size;
    auto tail = remote_tailers_[to_mid] - log_size;
    uint64_t base_offset = from_tid * thread_buf_size_ + from_mac * total_mac_log_size_ +
                           (tail % (thread_buf_size_ - log_entry_size_))
                           + base_offset_; // the start pointer of log area
    return base_offset;
  }

  inline char *get_local_log_ptr(int from_mac,int from_tid) {
    return (char *)local_buffer_ + from_mac * total_mac_log_size_ + from_tid * thread_buf_size_;
  }

  inline char *get_next_log(int from_mac,int from_tid,int size) {
    char *local_log_ptr = get_local_log_ptr(from_mac,from_tid);
    char *res = local_log_ptr + local_headers_[from_mac] % (thread_buf_size_ - log_entry_size_);
    local_headers_[from_mac] += size;
    return res;
  }

  // total number of machines need to store log at this server
  const int mac_num_;

  // total number of thread which will log at this server
  const int thread_num_;

  // total buffer used at each thread
  const int thread_buf_size_;

  // the start pointer of the log area
  const char *local_buffer_;

  const int log_entry_size_;

  // total log size used by one mac
  int total_mac_log_size_;



 private:
  // the remote tailer of the log
  // notice! this is a per-thread structure
  uint64_t *remote_tailers_ = NULL;
  // local received header
  uint64_t *local_headers_  = NULL;

  uint64_t base_offset_;
};
}; // namespace rtx

};   // namespace nocc
