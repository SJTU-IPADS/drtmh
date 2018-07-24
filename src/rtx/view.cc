#include "core/logging.h"
#include "view.h"

namespace nocc {

namespace rtx {

void SymmetricView::assign_backups(int total) {
  ASSERT(rep_factor_ <= RTX_MAX_BACKUP) << "Too many backups supported.";
  LOG(2) << "total " << total <<" backups to assign";
  for(uint i = 0;i < total;++i) {
    mapping_.emplace_back(rep_factor_);
    assign_one(i,total);
  }
}

void SymmetricView::assign_one(int idx,int total) {

  ASSERT(total > rep_factor_) << "Cannot assign backups properly. Total mac " << total
                              << "; rep factor " << rep_factor_;
  for(uint i = 0;i < rep_factor_;++i) {
    // uses static mapping.
    // maybe we can use a random strategy
    mapping_[idx][i] = (idx + i + 1) % (total);
  }
}

void SymmetricView::print() {
  for(uint i = 0;i < mapping_.size();++i) {
    LOG(2) << "Mac [" << i << "] backed by " << mapping_[i];
  }
}

} // namespace rtx

} // namespace nocc
