#include "bank_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"
#include "db/forkset.h"

#include "tx_config.h"

#include "util/util.h"

#include "rtx/occ_rdma.h"
#include "rtx/occ_variants.hpp"

#include <boost/bind.hpp>

#define unlikely(x) __builtin_expect(!!(x), 0)

extern nocc::db::TSManager *ts_manager;

extern __thread RemoteHelper *remote_helper;

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

void BankWorker::balance_piece(int id, int cid, char *input, yield_func_t &yield) {

  balance_req_header *header = (balance_req_header *)input;
  assert(false);
#ifdef OCC_TX
  RemoteHelper *h =  remote_helper;
  h->begin(_QP_ENCODE_ID(id,cid + 1));
#endif

#ifdef SI_TX
  uint64_t timestamp = (uint64_t)(&(header->ts_vec));
#else
  uint64_t timestamp = header->time;
#endif

  checking::value cv;
  savings::value sv;
  uint64_t seq = tx_->get_ro_versioned(CHECK,header->id,(char *)(&cv),timestamp,yield);
  seq = tx_->get_ro_versioned(SAV,header->id,(char *)(&sv),timestamp,yield);
  assert(seq != 1);

  double res = cv.c_balance + sv.s_balance;
  double *reply_msg  = (double *)(rpc_->get_reply_buf());
  *reply_msg = res;
  rpc_->send_reply((char *)reply_msg,sizeof(double), id, worker_id_,cid);
}

txn_result_t BankWorker::txn_balance2(yield_func_t &yield) {

  tx_->begin();
  uint64_t id;
retry:
  GetAccount(random_generator[cor_id_],&(id));
  int pid = AcctToPid(id);

  double res = 0.0;
  //if(pid != current_partition) {
  if(1){

    tx_->add_to_remote_set(CHECK,id,pid,yield);
    tx_->add_to_remote_set(SAV,id,pid,yield);

    tx_->do_remote_reads();
    indirect_yield(yield);
    tx_->get_remote_results(1);

    checking::value *cv;
    savings::value  *sv;
    tx_->get_cached(0,(char **)(&cv));
    tx_->get_cached(1,(char **)(&sv));

    res = cv->c_balance + sv->s_balance;

  } else {
    checking::value *cv;
    savings::value *sv;
    tx_->get(CHECK,id,(char **)(&cv),sizeof(checking::value));
    tx_->get(SAV,id,(char **)(&sv),sizeof(savings::value));
    res = cv->c_balance + sv->s_balance;
  }


  bool ret = tx_->end(yield);
  return txn_result_t(true,(uint64_t)res);
}

txn_result_t BankWorker::txn_deposit_checking(yield_func_t &yield) {

  tx_->begin(db_logger_);
  float amount = 1.3;
retry:
  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);

  checking::value *cv = NULL;

#ifndef EM_FASST
  tx_->add_to_remote_set(CHECK,id,pid,yield);
#else
  tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id);
#endif
  tx_->do_remote_reads();
  indirect_yield(yield);
#ifdef EM_FASST
  bool res = tx_->remoteset->get_results_readlock(1);
#else
  tx_->get_remote_results(1);
#endif

  uint64_t seq = tx_->get_cached(0,(char **)(&cv));
  cv->c_balance += 1;
  tx_->remote_write(0,(char *)cv,sizeof(checking::value));
#ifdef EM_FASST
  if(unlikely(res == false)) {
    // abort case
    tx_->remoteset->release_remote(yield);
    //tx_->remoteset->clear_for_reads();
    tx_->abort();
    return txn_result_t(false,0);
  }
  bool ret = tx_->end_fasst(yield);
#else
  bool ret = tx_->end(yield);
#endif

  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_send_payment(yield_func_t &yield) {

  tx_->begin(db_logger_);
  uint64_t id0,id1;
  GetTwoAccount(random_generator[cor_id_],&id0,&id1);
  float amount = 5.0;

  checking::value *c0, *c1; int idx0,idx1;

  int pid = AcctToPid(id0);
#ifdef EM_FASST
  tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id0);
#else
  tx_->add_to_remote_set(CHECK,id0,pid,yield);
#endif
  pid = AcctToPid(id1);
#ifdef EM_FASST
  tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id1);
#else
  tx_->add_to_remote_set(CHECK,id1,pid,yield);
#endif

#ifdef EM_FASST
  auto replies = tx_->remoteset->do_reads(2);
#else
  auto replies = tx_->do_remote_reads();
#endif

  indirect_yield(yield);

#ifdef EM_FASST
  bool ret = tx_->remoteset->get_results_readlock(replies);
#else
  tx_->get_remote_results(replies);
#endif

  auto seq = tx_->get_cached(0,(char **)(&c0));
  seq = tx_->get_cached(1,(char **)(&c1));

  if(c0->c_balance < amount) {
  } else {
    c0->c_balance -= amount;
    c1->c_balance += amount;
  }

  tx_->remote_write(0,(char *)c0,sizeof(checking::value));
  tx_->remote_write(1,(char *)c1,sizeof(checking::value));

#ifdef EM_FASST
  if(unlikely(!ret)) {
    tx_->remoteset->release_remote(yield);
    //tx_->remoteset->clear_for_reads();
    tx_->abort();
    return txn_result_t(false,0);
  }
  ret = tx_->end_fasst(yield);
#else
  auto ret = tx_->end(yield);
#endif
  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_transact_savings(yield_func_t &yield) {

  tx_->begin(db_logger_);
  float amount   = 20.20; //from original code
  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);

#ifdef EM_FASST
  tx_->remoteset->add(REQ_READ_LOCK,pid,SAV,id);
  tx_->remoteset->do_reads(1);
#else
  tx_->add_to_remote_set(SAV,id,pid,yield);
  tx_->do_remote_reads();
#endif

  indirect_yield(yield);


#ifdef EM_FASST
  bool ret = tx_->remoteset->get_results_readlock(1);
#else
  tx_->get_remote_results(1);
#endif

  savings::value *sv;
  uint64_t seq = tx_->get_cached(0,(char **)(&sv));
  sv->s_balance += amount;
  tx_->remote_write(0,(char *)sv,sizeof(savings::value));
#ifdef EM_FASST
  if(unlikely(!ret)) {
    tx_->remoteset->release_remote(yield);
    //tx_->remoteset->clear_for_reads();
    tx_->abort();
    return txn_result_t(false,0);
  }

  ret = tx_->end_fasst(yield);
#else
  auto ret = tx_->end(yield);
#endif
  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_write_check(yield_func_t &yield) {

  tx_->begin(db_logger_);
  float amount = 5.0; //from original code

  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);

#ifdef EM_FASST
  tx_->remoteset->add(REQ_READ,pid,SAV,id);
  tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id);
#else
  tx_->add_to_remote_set(SAV,id,pid,yield);
  tx_->add_to_remote_set(CHECK,id,pid,yield);
#endif
  //auto replies = tx_->remoteset->do_reads(0);
  auto replies = tx_->do_remote_reads();

  indirect_yield(yield);

#ifdef EM_FASST
  bool res = tx_->remoteset->get_results_readlock(replies);
#else
  tx_->get_remote_results(replies);
#endif

  savings::value *sv;
  checking::value *cv;
  auto seq = tx_->get_cached(0,(char **)(&sv));
  seq = tx_->get_cached(1,(char **)(&cv));

  auto total = sv->s_balance + cv->c_balance;
  if(total < amount) {
    cv->c_balance -= (amount - 1);
  } else
    cv->c_balance -= amount;

  tx_->remote_write(1,(char *)cv,sizeof(checking::value));

#ifdef EM_FASST
  if(unlikely(!res)) {
    tx_->remoteset->release_remote(yield);
    //tx_->remoteset->clear_for_reads();
    tx_->abort();
    return txn_result_t(false,0);
  }
#endif

#ifdef EM_FASST
  bool ret = tx_->end_fasst(yield);
#else
  bool ret = tx_->end(yield);
#endif
  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_amal(yield_func_t &yield) {

  tx_->begin(db_logger_);
  uint64_t id0,id1;
retry:
  GetTwoAccount(random_generator[cor_id_],&id0,&id1);

  checking::value *c0,*c1; savings::value *s0,s1;

  int pid0 = AcctToPid(id0);
  int pid1 = AcctToPid(id1),idx1;
#ifdef EM_FASST
  tx_->remoteset->add(REQ_READ_LOCK,pid0,SAV,id0);
  tx_->remoteset->add(REQ_READ_LOCK,pid0,CHECK,id0);
  tx_->remoteset->add(REQ_READ_LOCK,pid1,CHECK,id1);
#else
  tx_->add_to_remote_set(SAV,id0,pid0,yield);
  tx_->add_to_remote_set(CHECK,id0,pid0,yield);
  tx_->add_to_remote_set(CHECK,id1,pid1,yield);
#endif

  auto replies = tx_->do_remote_reads();

  indirect_yield(yield);
#ifdef EM_FASST
  bool res = tx_->remoteset->get_results_readlock(replies);
#else
  tx_->get_remote_results(replies);
#endif

  double total = 0;

  auto seq = tx_->get_cached(0,(char **)(&s0));
  //assert(seq != 0);
  seq = tx_->get_cached(1,(char **)(&c0));
  //        assert(seq != 0);

  total = s0->s_balance + c0->c_balance;

  s0->s_balance = 0;
  c0->c_balance = 0;

  tx_->remote_write(0,(char *)s0,sizeof(savings::value));
  tx_->remote_write(1,(char *)c0,sizeof(checking::value));

  seq = tx_->get_cached(2,(char **)(&c1));
  //assert(seq != 0);
  c1->c_balance += total;
  tx_->remote_write(2,(char *)c1,sizeof(checking::value));

#ifdef EM_FASST
  if(unlikely(!res)) {
    // no early aborts here
    tx_->remoteset->release_remote(yield);
    //tx_->remoteset->clear_for_reads();
    tx_->abort();
    return txn_result_t(false,0);
  }

  bool ret = tx_->end_fasst(yield);
#else
  bool ret = tx_->end(yield);
#endif
  return txn_result_t(ret,73); // since readlock success, so no need to abort
}



void BankWorker::thread_local_init() {

  assert(store_ != NULL);
  for(uint i = 0;i < server_routine + 1;++i) {
    // init TXs
#ifdef RAD_TX
    txs_[i] = new DBRad(store_,worker_id_,rpc_,i);
#elif defined(OCC_TX)
    //txs_[i] = new DBTX(store_,worker_id_,rpc_,i);
    //((DBTX *)(txs_[i]))->new_logger_ = new_logger_;
    //remote_helper = new RemoteHelper(store_,total_partition,server_routine + 1);

    // -1 transformed every local operations to remote ones
#if EM_FASST == 0
#if ONE_SIDED_READ
    new_txs_[i] = new rtx::OCCR(this,store_,rpc_,current_partition,worker_id_,i,-1,
                                   cm,rdma_sched_,total_partition);
#else
    new_txs_[i] = new rtx::OCC(this,store_,rpc_,current_partition,i,-1);
#endif
#else
    new_txs_[i] = new rtx::OCCFast(this,store_,rpc_,current_partition,i,-1);
#endif
    new_txs_[i]->set_logger(new_logger_);

#elif defined(FARM)
    txs_[i] = new DBFarm(cm,rdma_sched_,store_,worker_id_,rpc_,i);
#elif defined(SI_TX)
    txs_[i] = new DBSI(store_,worker_id_,rpc_,i);
#else
    fprintf(stderr,"No transaction layer used!\n");
    assert(false);
#endif
  }
  /* init local tx so that it is not a null value */
  tx_ = txs_[cor_id_];
  rtx_ = new_txs_[cor_id_];
  rtx_hook_ = new_txs_[1];
  //routine_1_tx_ = txs_[1]; // used for report
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
