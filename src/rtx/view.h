#ifndef RTX_VIEW_H_
#define RTX_VIEW_H_

#include <string>
#include <vector>
#include <algorithm>
#include <set>

#include "core/logging.h"

namespace nocc {

namespace rtx {

#define RTX_MAX_BACKUP 2

struct BackupInfo {
  int mapping[RTX_MAX_BACKUP];
  const int rep_factor;

  explicit BackupInfo(int rep)
      :rep_factor(rep)
  {
    std::fill_n(mapping,RTX_MAX_BACKUP,-1);
  }

  inline int& operator[] (std::size_t idx) {
    return mapping[idx];
  }

  friend std::ostream& operator<<(std::ostream& ss, const BackupInfo& b) {
    for(uint i = 0;i < b.rep_factor;++i)
      ss << b.mapping[i] << ",";
    ss << ".";
  }
};

class SymmetricView {
 public:
  SymmetricView(int rep_factor,int total_mac) :
      rep_factor_(rep_factor >= total_mac?0:rep_factor)
  {
    if(rep_factor_ >= total_mac) {
      LOG(3) << "Disable backups!"
             << "Rep factor requires at least " << rep_factor_ + 1 <<" macs,"
             << "yet total " << total_mac << "in the setting.";
    }
    LOG(3) << "Start with " << rep_factor_ << " backups.";
    assign_backups(total_mac);
  }

  /**
   * add pid's backup list to the mac_set.
   */
  inline void add_backup(int pid,std::set<int> &mac_set) {
    for(uint i = 0;i < rep_factor_;++i)
      mac_set.insert(mapping_[pid][i]);
  }

  /**
   * If bid is one of pid's backup, return true.
   * Otherwise, return false.
   */
  inline bool is_backup(int bid,int pid) {
    return is_backup(bid,pid,rep_factor_);
  }

  inline size_t response_for(int bid,std::set<int> &set) {
    for(uint i = 0;i < mapping_.size();++i)
      if(is_backup(bid,i))
        set.insert(i);
    return set.size();
  }

  /**
   * If bid is one of pid's backup in the first iterating_num mac, return true.
   * Otherwise, return false.
   */
  inline bool is_backup(int bid,int pid,int iterating_num) {
    for(uint i = 0;i < iterating_num;++i)
      if(mapping_[pid][i] == bid)
        return true;
    return false;
  }

  void print();

  const int rep_factor_;

 private:
  std::vector<BackupInfo> mapping_;

  void assign_backups(int total);
  void assign_one(int idx,int total);


  DISABLE_COPY_AND_ASSIGN(SymmetricView);
}; // SymmetricView

} // namespace rtx

} // namespace nocc


#endif
