#pragma once

#include "util/util.h"

namespace nocc {

class SimpleCache {
 public:
  SimpleCache(int size) : size_(size)
  {
    if(size != 0) {
      content_ =  (uint64_t *)malloc_huge_pages(size,HUGE_PAGE_SZ,true);
      assert(content_ != NULL);
    }
  }

  void put(uint64_t key,uint64_t off) {
    assert(key < size_ / sizeof(uint64_t));
    content_[key] = off;
  }

  inline uint64_t get(uint64_t key) {
    return content_[key];
  }
 private:
  uint64_t *content_ = NULL;
    int size_;
};

}; // namespace nocc
