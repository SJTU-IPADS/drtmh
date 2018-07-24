#ifndef NOCC_OLTP_BANK_H
#define NOCC_OLTP_BANK_H


#include "all.h"

#include "tx_config.h"
#include "app/config.h"

#include "bank_schema.h"

#include "memstore/memdb.h"

#include "framework/backup_worker.h"
#include "framework/bench_worker.h"
#include "framework/utils/util.h"

#include "db/txs/tx_handler.h"

#include "micautil/hash.h" // ensure the distribution is the same as FaSST
#include <string>

#define DEFAULT_NUM_ACCOUNTS 100000  // Accounts per partition
//#define DEFAULT_NUM_ACCOUNTS 10 // small scale for tests
#define DEFAULT_NUM_HOT 4000  // Hot accounts per partition
//#define DEFAULT_NUM_HOT 5  // small scale for tests

#define TX_HOT 90 // Percentage of txns that use accounts from hotspot


extern size_t total_partition;
//using namespace util;
namespace nocc {
namespace oltp {
namespace bank {

struct balance_req_header {
  uint64_t id;
#ifdef SI_TX
  uint64_t ts_vec[16];
#else
  uint64_t time;
#endif
};

void BankTest(int argc,char **argv);

void GetAccount(util::fast_random &r, uint64_t *acct_id);
void GetTwoAccount(util::fast_random &r, uint64_t *acct_id_0, uint64_t *acct_id_1);
inline ALWAYS_INLINE int  AcctToPid(uint64_t id) {
  return mica::util::hash(&id, sizeof(uint64_t)) % total_partition;
}

inline ALWAYS_INLINE int
_CheckBetweenInclusive(int v, int lower, int upper)
{
  INVARIANT(v >= lower);
  INVARIANT(v <= upper);
  return v;
}

inline ALWAYS_INLINE int
_RandomNumber(util::fast_random &r, int min, int max)
{
  return _CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
}

uint64_t NumAccounts();
uint64_t NumHotAccounts();
uint64_t GetStartAcct();
uint64_t GetEndAcct();

/* Tx's implementation */
class BankWorker : public BenchWorker {
 public:
  BankWorker(unsigned int worker_id,unsigned long seed,MemDB *db,uint64_t total_ops,
             spin_barrier *a, spin_barrier *b,BenchRunner *context);

  txn_result_t txn_send_payment(yield_func_t & yield) ;
  txn_result_t txn_sp_new(yield_func_t &yield);

  txn_result_t txn_deposit_checking(yield_func_t &yield);
  txn_result_t txn_dc_new(yield_func_t &yield);

  txn_result_t txn_balance2(yield_func_t &yield);
  txn_result_t txn_balance_new(yield_func_t &yield);

  txn_result_t txn_transact_savings(yield_func_t &yield);
  txn_result_t txn_ts_new(yield_func_t &yield);

  txn_result_t txn_write_check(yield_func_t &yield);
  txn_result_t txn_wc_new(yield_func_t &yield);

  txn_result_t txn_amal(yield_func_t &yield);
  txn_result_t txn_amal_new(yield_func_t &yield);

  void balance_piece(int id,int cid,char *input,yield_func_t &yield);

  virtual workload_desc_vec_t get_workload() const ;
  static  workload_desc_vec_t _get_workload();
  virtual void check_consistency();
  virtual void register_callbacks();
  virtual void thread_local_init();

  uint64_t server_heatmap[4];

 private:
  MemDB *store_;
  std::map<int,int> mac_hotmap;

  static txn_result_t TxnSendPayment(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<BankWorker *>(w)->txn_sp_new(yield);
    return r;
  }

  static txn_result_t TxnDepositChecking(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<BankWorker *>(w)->txn_dc_new(yield);
    return r;
  }

  static txn_result_t TxnBalance(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<BankWorker *>(w)->txn_balance_new(yield);
    return r;
  }

  static txn_result_t TxnTransactSavings(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<BankWorker *>(w)->txn_ts_new(yield);
    return r;
  }

  static txn_result_t TxnWriteCheck(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<BankWorker *>(w)->txn_wc_new(yield);
    return r;
  }

  static txn_result_t TxnAmal(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<BankWorker *>(w)->txn_amal_new(yield);
    return r;
  }

};
} // end namespace bank
} // end namespace oltp
}

#endif
