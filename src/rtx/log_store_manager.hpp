#pragma once

#include "memstore/cluster_chaining.hpp"
#include "memstore/memdb.h"

namespace nocc {
namespace rtx {

#define RTX_BACKUP_MAX 4 // maxinum backups stored at this server

class LogStoreManager {
  typedef drtm::ClusterHash<MemDB *,1> store_map_t;
 public:
  explicit LogStoreManager(int expected_backup_num = RTX_BACKUP_MAX)
      :backup_stores_(expected_backup_num)
  {
  }

  // add a particular store to a specific DB
  void add_backup_store(int id,MemDB *db) {
    assert(backup_stores_.get(id) == NULL);
    auto new_val_p = backup_stores_.get_with_insert(id);
    *new_val_p = db;
  }

  MemDB *get_backed_store(int id) {
    return (*backup_stores_.get(id));
  }

 protected:
  store_map_t backup_stores_;
};

}; // namespace rtx

};

