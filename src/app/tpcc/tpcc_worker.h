#ifndef NOCC_OLTP_TPCC_WORKER_H
#define NOCC_OLTP_TPCC_WORKER_H

#include "all.h"
#include "tx_config.h"

#include "tpcc_schema.h"
#include "tpcc_mixin.h"
#include "memstore/memdb.h"

#include "framework/bench_worker.h"

#include "core/utils/latency_profier.h"

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
#define META_LENGTH 16

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

  txn_result_t txn_new_order_new(yield_func_t &yield);

  const uint warehouse_id_start_ ;
  const uint warehouse_id_end_ ;
  uint64_t last_no_o_ids_[1024][10];
  LAT_VARS(read);

 public:
  virtual workload_desc_vec_t get_workload() const ;
  static  workload_desc_vec_t _get_workload();

  virtual void thread_local_init();
  virtual void register_callbacks();

  virtual void workload_report() {
    double res;
    REPORT_V(read,res);
    latencys_.push_back(res);

    // record TX's data
    rtx_hook_->record();
  }

  void exit_report() {
    LOG(4) << "worker exit.";
    latencys_.erase(0.2);
    auto one_second = util::BreakdownTimer::get_one_second_cycle();
    LOG(4) << "read time: " << util::BreakdownTimer::rdtsc_to_ms(latencys_.average(),one_second) << "ms";

    rtx_hook_->report_statics(one_second);
  }

  /* Wrapper for implementation of transaction */
  static txn_result_t TxnNewOrder(BenchWorker *w,yield_func_t &yield) {
    txn_result_t r = static_cast<TpccWorker *>(w)->txn_new_order_new(yield);
    return r;
  }

  static txn_result_t TxnPayment(BenchWorker *w,yield_func_t &yield) {
    return txn_result_t(false,1);
  }

  static txn_result_t TxnDelivery(BenchWorker *w,yield_func_t &yield) {
    return txn_result_t(false,1);
  }

  static txn_result_t TxnStockLevel(BenchWorker *w,yield_func_t &yield) {
    return txn_result_t(false,1);
  }

  static txn_result_t TxnSuperStockLevel(BenchWorker *w,yield_func_t &yield) {
    return txn_result_t(false,1);
  }

  static txn_result_t TxnOrderStatus(BenchWorker *w,yield_func_t &yield) {
    return txn_result_t(false,1);
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
