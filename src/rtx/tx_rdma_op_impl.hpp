#include "tx_config.h"

namespace nocc {
namespace rtx  {

inline __attribute__((always_inline))
uint64_t TXOpBase::rdma_lookup_op(int pid,int tableid,uint64_t key,char *val,
                                  yield_func_t &yield,int meta_len) {

  Qp* qp = qp_vec_[pid];
  assert(qp != NULL);

  // MemNode will be stored in val, if necessary
  auto off = db_->stores_[tableid]->RemoteTraverse(key,qp,scheduler_,yield,val);
  return off;
}


} // namespace rtx

} // namespace nocc
