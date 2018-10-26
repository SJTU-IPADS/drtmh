#include "tx_config.h"

#include "tpcc_worker.h"

#include <set>
#include <limits>
#include <boost/bind.hpp>

#include "rtx/occ_rdma.h"
#include "rtx/occ_variants.hpp"

#define MICRO_DIST_NUM 100

extern size_t nclients;
extern size_t current_partition;
extern size_t total_partition;

//#define RC

using namespace nocc::util;

namespace nocc {

extern RdmaCtrl *cm;

namespace oltp {

extern __thread util::fast_random   *random_generator;

namespace tpcc {

uint64_t npayment_executed = 0;
extern unsigned g_txn_workload_mix[5];
extern int g_new_order_remote_item_pct;
extern int g_mico_dist_num;

// some debug info
//std::map<uint64_t,int> order_num;

TpccWorker::TpccWorker(unsigned int worker_id,unsigned long seed,
                       uint warehouse_id_start,uint warehouse_id_end,MemDB *store,uint64_t total_ops,
                       spin_barrier *a,spin_barrier *b,BenchRunner *context)
    : TpccMixin(store),
      BenchWorker(worker_id,true,seed,total_ops,a,b,context),
      warehouse_id_start_(warehouse_id_start),
      warehouse_id_end_(warehouse_id_end)
{
  assert(NumWarehouses() <= 1024);
  // init last warehouse id
  for(uint w = 0;w < NumWarehouses() + 1;++w) {
    for(uint d = 0;d < NumDistrictsPerWarehouse();++d) {
      last_no_o_ids_[w][d] = 0; // 3000 is magic number, which is the init num of new order
    }
  }

  // init lat profiling variables
  INIT_LAT_VARS(read);
}

void TpccWorker::register_callbacks() {
}

void TpccWorker::thread_local_init() {

  for(uint i = 0;i < server_routine + 1;++i) {

#if ONE_SIDED_READ
    new_txs_[i] = new rtx::OCCR(this,store_,rpc_,current_partition,worker_id_,i,current_partition,
                                cm,rdma_sched_,total_partition);
#else // the case for RPC
#if EM_FASST == 0
    new_txs_[i] = new rtx::OCC(this,store_,rpc_,current_partition,i,current_partition);
#else
    new_txs_[i] = new rtx::OCCFast(this,store_,rpc_,current_partition,i,current_partition);
#endif
#endif

    new_txs_[i]->set_logger(new_logger_);
  }
  rtx_hook_ = new_txs_[1];
  ASSERT(new_logger_ != nullptr);
}

workload_desc_vec_t TpccWorker::get_workload() const {
  return _get_workload();
}

workload_desc_vec_t TpccWorker::_get_workload() {

  workload_desc_vec_t w;
  unsigned m = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
    m += g_txn_workload_mix[i];
  ALWAYS_ASSERT(m == 100);

  if (g_txn_workload_mix[0])
    w.push_back(workload_desc("NewOrder", double(g_txn_workload_mix[0])/100.0, TxnNewOrder));
  if (g_txn_workload_mix[1])
    w.push_back(workload_desc("Payment", double(g_txn_workload_mix[1])/100.0, TxnPayment));
  if (g_txn_workload_mix[2])
    w.push_back(workload_desc("Delivery", double(g_txn_workload_mix[2])/100.0, TxnDelivery));
  if (g_txn_workload_mix[3])
    w.push_back(workload_desc("OrderStatus", double(g_txn_workload_mix[3])/100.0, TxnOrderStatus));
  if (g_txn_workload_mix[4])
    w.push_back(workload_desc("StockLevel", double(g_txn_workload_mix[4])/100.0, TxnStockLevel));

  return w;
}


} // namespace tpcc
} // namespace oltp
} // namespace nocc
