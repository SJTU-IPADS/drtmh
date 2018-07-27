// implementations of smallbank on revised codebase

#include "bank_worker.h"
#include "tx_config.h"

namespace nocc {

namespace oltp {

extern __thread util::fast_random   *random_generator;

bool verify_check_balance(const checking::value *c) {
  return c->c_balance >= MIN_BALANCE && c->c_balance <= MAX_BALANCE;
}

bool verify_save_balance(const savings::value *s) {
  return s->s_balance >= MIN_BALANCE && s->s_balance <= MAX_BALANCE;
}

namespace bank {

txn_result_t BankWorker::txn_sp_new(yield_func_t &yield) {

  rtx_->begin(yield);

  uint64_t id0,id1;
  GetTwoAccount(random_generator[cor_id_],&id0,&id1);
  float amount = 5.0;

  // first account
  int pid = AcctToPid(id0);
#if EM_FASST == 0
  rtx_->add_to_read<CHECK,checking::value>(pid,id0,yield);
#else
  rtx_->add_to_write<CHECK,checking::value>(pid,id0,yield);
#endif
  // second account
  pid = AcctToPid(id1);
#if EM_FASST == 0
  rtx_->add_to_read<CHECK,checking::value>(pid,id1,yield);
#else
  rtx_->add_to_write<CHECK,checking::value>(pid,id1,yield);
#endif

  auto c0 = rtx_->get_readset<checking::value>(0,yield);
  auto c1 = rtx_->get_readset<checking::value>(1,yield);

  if(c0->c_balance < amount) {
#if EM_FASST == 1
    rtx_->add_to_write(1);
    rtx_->add_to_write(0);
#endif
  } else {
    c0->c_balance -= amount;
    c1->c_balance += amount;
    rtx_->add_to_write(1);
    rtx_->add_to_write(0);
  }
  auto ret = rtx_->commit(yield);
  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_wc_new(yield_func_t &yield) {

  rtx_->begin(yield);

  float amount = 5.0; //from original code

  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);
#if EM_FASST == 0
  rtx_->add_to_read<SAV,savings::value>(pid,id,yield);
  rtx_->add_to_read<CHECK,checking::value>(pid,id,yield);
#else
  rtx_->add_to_read<SAV,savings::value>(pid,id,yield);
  rtx_->add_to_write<CHECK,checking::value>(pid,id,yield);
#endif

  savings::value  *sv = rtx_->get_readset<savings::value>(0,yield);
  checking::value *cv = rtx_->get_readset<checking::value>(1,yield);

  auto total = sv->s_balance + cv->c_balance;
  if(total < amount) {
    cv->c_balance -= (amount - 1);
  } else {
    cv->c_balance -= amount;
  }
  rtx_->add_to_write();

  auto   ret = rtx_->commit(yield);
  return txn_result_t(ret,1);
}

txn_result_t BankWorker::txn_dc_new(yield_func_t &yield) {

  rtx_->begin(yield);

  float amount = 1.3;
retry:
  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);

#if EM_FASST == 0
  rtx_->add_to_read<CHECK,checking::value>(pid,id,yield);
#else
  rtx_->add_to_write<CHECK,checking::value>(pid,id,yield);
#endif

  checking::value *cv = rtx_->get_readset<checking::value>(0,yield);

  // fetch cached record from read-set
  assert(cv != NULL);
  cv->c_balance += amount;
  rtx_->add_to_write();

  bool ret = rtx_->commit(yield);

#if 0 // check the correctness
  {
    if(ret == true) {
      rtx_->begin();
      rtx_->start_batch_read();
      rtx_->add_batch_read(CHECK,id,pid,sizeof(checking::value));
      rtx_->send_batch_read();
      checking::value *cv1 = rtx_->get_readset<checking::value>(0,yield);
      ASSERT(cv1->c_balance == cv->c_balance) << "exe return: " << cv->c_balance
                                              << "; check return: " << cv1->c_balance;
    }
  }
#endif

  return txn_result_t(ret,1);
}

txn_result_t BankWorker::txn_ts_new(yield_func_t &yield) {

  rtx_->begin(yield);

  float amount   = 20.20; //from original code
  uint64_t id;
  GetAccount(random_generator[cor_id_],&id);
  int pid = AcctToPid(id);
#if EM_FASST == 0
  rtx_->add_to_read<SAV,savings::value>(pid,id,yield);
#else
  rtx_->add_to_write<SAV,savings::value>(pid,id,yield);
#endif

  auto sv = rtx_->get_readset<savings::value>(0,yield);

  sv->s_balance += amount;
  rtx_->add_to_write(0);
  auto ret = rtx_->commit(yield);
  return txn_result_t(ret,73);
}

txn_result_t BankWorker::txn_balance_new(yield_func_t &yield) {

  rtx_->begin(yield);

  uint64_t id;
  GetAccount(random_generator[cor_id_],&(id));
  int pid = AcctToPid(id);

  double res = 0.0;
  rtx_->add_to_read<CHECK,checking::value>(pid,id,yield);
  rtx_->add_to_read<SAV,savings::value>(pid,id,yield);

  auto cv = rtx_->get_readset<checking::value>(0,yield);
  auto sv = rtx_->get_readset<savings::value>(1,yield);
  res = cv->c_balance + sv->s_balance;

  bool ret = rtx_->commit(yield);
  return txn_result_t(ret,(uint64_t)0);
}


txn_result_t BankWorker::txn_amal_new(yield_func_t &yield) {

  rtx_->begin(yield);

  uint64_t id0,id1;
  GetTwoAccount(random_generator[cor_id_],&id0,&id1);

  int pid0 = AcctToPid(id0);
  int pid1 = AcctToPid(id1),idx1;

#if EM_FASST == 0
  rtx_->add_to_read<SAV,savings::value>(pid0,id0,yield);
  rtx_->add_to_read<CHECK,checking::value>(pid0,id0,yield);
  rtx_->add_to_read<CHECK,checking::value>(pid1,id1,yield);
#else
  rtx_->add_to_write<SAV,savings::value>(pid0,id0,yield);
  rtx_->add_to_write<CHECK,checking::value>(pid0,id0,yield);
  rtx_->add_to_write<CHECK,checking::value>(pid1,id1,yield);
#endif

  auto s0 = rtx_->get_readset<savings::value>(0,yield);
  auto c0 = rtx_->get_readset<checking::value>(1,yield);
  auto c1 = rtx_->get_readset<checking::value>(2,yield);

  double total = 0;

  total = s0->s_balance + c0->c_balance;

  s0->s_balance = 0;
  c0->c_balance = 0;

  c1->c_balance += total;

  rtx_->add_to_write(2);
  rtx_->add_to_write(1);
  rtx_->add_to_write(0);

  auto ret = rtx_->commit(yield);
  return txn_result_t(ret,73); // since readlock success, so no need to abort
}

}; // namespace bank

}; // namespace oltp

};
