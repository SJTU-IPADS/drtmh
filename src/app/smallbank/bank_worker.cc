#include "bank_worker.h"

#include "tx_config.h"

#include "util/util.h"

#include "rtx/occ_rdma.h"
#include "rtx/occ_variants.hpp"

#include <boost/bind.hpp>

#define unlikely(x) __builtin_expect(!!(x), 0)

extern size_t current_partition;

namespace nocc {

extern RdmaCtrl *cm;

namespace oltp {

extern __thread util::fast_random   *random_generator;

namespace bank {

BreakdownTimer compute_timer;
BreakdownTimer send_timer;

extern unsigned g_txn_workload_mix[6];

/* input generation */
void GetAccount(util::fast_random &r, uint64_t *acct_id) {
  uint64_t nums_global;
  if(r.next() % 100 < TX_HOT) {
    nums_global = NumAccounts();
  } else {
    nums_global = NumHotAccounts();
  }
  *acct_id = r.next() % nums_global;
}

void GetTwoAccount(util::fast_random &r,
                   uint64_t *acct_id_0, uint64_t *acct_id_1)  {
  uint64_t nums_global;
  if(r.next() % 100 < TX_HOT) {
    nums_global = NumAccounts();
  } else {
    nums_global = NumHotAccounts();
  }
  *acct_id_0 = r.next() % nums_global;
  *acct_id_1 = r.next() % nums_global;
  while(*acct_id_1 == *acct_id_0) {
    *acct_id_1 = r.next() % nums_global;
  }
}


BankWorker::BankWorker(unsigned int id,unsigned long seed,MemDB *db,uint64_t total_ops,
                       spin_barrier *a, spin_barrier *b,BenchRunner *context):
    BenchWorker(id,true,seed,total_ops,a,b,context),
    store_(db)
{
  // clear timer states
  compute_timer.report();
  send_timer.report();
}

void BankWorker::register_callbacks() {
}

void BankWorker::check_consistency() {

}

void BankWorker::thread_local_init() {

  assert(store_ != NULL);
  for(uint i = 0;i < server_routine + 1;++i) {
    // init TXs

#if ONE_SIDED_READ
    new_txs_[i] = new rtx::OCCR(this,store_,rpc_,current_partition,worker_id_,i,current_partition,
                                cm,rdma_sched_,total_partition);
#else // the case for RPC
#if EM_FASST == 0
    new_txs_[i] = new rtx::OCC(this,store_,rpc_,current_partition,i,-1);
#else
#if ONE_SIDED_READ
    new_txs_[i] = new rtx::OCCFastR(this,store_,rpc_,current_partition,worker_id_,i,-1,
                                    cm,rdma_sched_,total_partition);
#else
    new_txs_[i] = new rtx::OCCFast(this,store_,rpc_,current_partition,i,-1);
#endif
#endif
#endif
    new_txs_[i]->set_logger(new_logger_);
  }
  /* init local tx so that it is not a null value */
  rtx_ = new_txs_[cor_id_];
  rtx_hook_ = new_txs_[1];
} // end func: thread_local_init


workload_desc_vec_t BankWorker::get_workload() const {
  return _get_workload();
}

workload_desc_vec_t BankWorker::_get_workload() {

  workload_desc_vec_t w;
  unsigned m = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
    m += g_txn_workload_mix[i];
  ALWAYS_ASSERT(m == 100);

  if(g_txn_workload_mix[0]) {
    w.push_back(workload_desc("SendPayment", double(g_txn_workload_mix[0])/100.0, TxnSendPayment));
  }
  if(g_txn_workload_mix[1]) {
    w.push_back(workload_desc("DepositChecking",double(g_txn_workload_mix[1])/100.0,TxnDepositChecking));
  }
  if(g_txn_workload_mix[2]) {
    w.push_back(workload_desc("Balance",double(g_txn_workload_mix[2])/100.0,TxnBalance));
  }
  if(g_txn_workload_mix[3]) {
    w.push_back(workload_desc("Transact saving",double(g_txn_workload_mix[3])/100.0,TxnTransactSavings));
  }
  if(g_txn_workload_mix[4]) {
    w.push_back(workload_desc("Write check",double(g_txn_workload_mix[4])/100.0,TxnWriteCheck));
  }
  if(g_txn_workload_mix[5]) {
    w.push_back(workload_desc("Txn amal",double(g_txn_workload_mix[5])/100.0,TxnAmal));
  }
  return w;
}


}; // namespace bank
};
};
