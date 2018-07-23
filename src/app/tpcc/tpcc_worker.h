#ifndef NOCC_OLTP_TPCC_WORKER_H
#define NOCC_OLTP_TPCC_WORKER_H

#include "all.h"
#include "tx_config.h"
#include "./app/config.h"

#include "tpcc_schema.h"
#include "tpcc_mixin.h"
#include "memstore/memdb.h"

#include "framework/backup_worker.h"
#include "framework/bench_worker.h"

#include "db/txs/tx_handler.h"
#include "db/db_statistics_helper.h"

#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"

#define MAX_DIST 10

#define TX_STOCK_LEVEL 0
#define STOCK_LEVEL_ORDER_COUNT 10 /* shall be 20 by default */

/* which concurrency control code to use */

/* System headers */
#include <string>

namespace nocc {
namespace oltp {

namespace tpcc {

// multi-version store can use dedicated meta length
#ifdef RAD_TX
#undef META_LENGTH
#define META_LENGTH RAD_META_LEN
#endif

#ifdef SI_TX
#undef META_LENGTH
#define META_LENGTH SI_META_LEN
#endif

/* Main function */
void TpccTest(int argc,char **argv);
int  GetStartWarehouse(int partition);
int  GetEndWarehouse(int partition);
int  NumWarehouses();
int  WarehouseToPartition(int wid);

struct StockLevelInput {
  int8_t num;
  int32_t threshold;
  uint64_t tx_id;
#ifdef SI_TX
  uint64_t ts_vec[12]; //FIXME!! hard coded
#else
  uint64_t timestamp;
#endif
};
struct StockLevelReply {
  int32_t num_items;
};
struct StockLevelInputPayload {
  uint16_t warehouse_id;
  uint8_t district_id;
  uint8_t pid;
};

/* Tx's implementation */
class TpccWorker : public TpccMixin, public BenchWorker {

 public:
  TpccWorker(unsigned int worker_id,unsigned long seed,uint warehouse_id_start,uint warehouse_id_end,
             MemDB *db,uint64_t total_ops,
             spin_barrier *a,spin_barrier *b,
             BenchRunner *context);
  ~TpccWorker();

  txn_result_t txn_new_order(yield_func_t &yield);
  txn_result_t txn_new_order_new(yield_func_t &yield);

  txn_result_t txn_payment(yield_func_t &yield);
  txn_result_t txn_delivery(yield_func_t &yield);
  txn_result_t txn_stock_level(yield_func_t &yield);
  txn_result_t txn_order_status(yield_func_t &yield);
  txn_result_t txn_super_stock_level(yield_func_t &yield);

  txn_result_t txn_micro(yield_func_t &yield);

  // naive version of 2 TXs
  txn_result_t txn_new_order_naive(yield_func_t &yield);  // non speculative execution
  txn_result_t txn_new_order_naive1(yield_func_t &yield); // + batching

  txn_result_t txn_payment_naive(yield_func_t &yield);    // non speculative execution
  txn_result_t txn_payment_naive1(yield_func_t &yield);   // + batching


  /* The later are used for transactions handling ro fork/join modes */
  void stock_level_piece(yield_func_t &yield,int id,int cid,char* input);

 private:
  const uint warehouse_id_start_ ;
  const uint warehouse_id_end_ ;
  uint64_t last_no_o_ids_[1024][10];
  LAT_VARS(read);

 public:
  virtual workload_desc_vec_t get_workload() const ;
  static  workload_desc_vec_t _get_workload();
  virtual void check_consistency();
  virtual void thread_local_init();
  virtual void register_callbacks();

  virtual void workload_report() {
    BenchWorker::workload_report();
    REPORT(read);
  }


  /* Wrapper for implementation of transaction */
  static txn_result_t TxnNewOrder(BenchWorker *w,yield_func_t &yield) {
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order(yield);
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order_new(yield);
    return r;
  }

  static txn_result_t TxnPayment(BenchWorker *w,yield_func_t &yield) {
#if NAIVE == 1
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive(yield);
#elif NAIVE == 2 || NAIVE == 3
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment_naive1(yield);
#else
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_payment(yield);
#endif
    return r;
  }

  static txn_result_t TxnDelivery(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_delivery(yield);
    return r;
  }

  static txn_result_t TxnStockLevel(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_stock_level(yield);
    //txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level(yield);
    return r;
  }

  static txn_result_t TxnSuperStockLevel(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_super_stock_level(yield);
    return r;
  }

  static txn_result_t TxnOrderStatus(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_order_status(yield);
    return r;
  }
};

/* Loaders */
class TpccWarehouseLoader : public BenchLoader, public TpccMixin {
 public:
  TpccWarehouseLoader(unsigned long seed, int partition,MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccDistrictLoader : public BenchLoader, public TpccMixin {
 public:
  TpccDistrictLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccCustomerLoader : public BenchLoader, public TpccMixin {
 public:
  TpccCustomerLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccOrderLoader : public BenchLoader, public TpccMixin {
 public:
  TpccOrderLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccItemLoader : public BenchLoader, public TpccMixin {
 public:
  TpccItemLoader(unsigned long seed, int partition, MemDB *store)
      : BenchLoader(seed),
        TpccMixin(store) {

    partition_ = partition;
  }
 protected:
  virtual void load();
};

class TpccStockLoader : public BenchLoader, public TpccMixin {
 public:
  TpccStockLoader(unsigned long seed, int partition, MemDB *store,bool backup = false)
      : BenchLoader(seed),
        TpccMixin(store),backup_(backup) {

    partition_ = partition;
  }
  const bool backup_;
 protected:
  virtual void load();
};

}
}
}
#endif
