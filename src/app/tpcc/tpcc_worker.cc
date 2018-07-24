#include "tx_config.h"

#include "tpcc_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/db_farm.h"
#include "db/txs/dbsi.h"

#include "db/forkset.h"

#include <set>
#include <limits>
#include <boost/bind.hpp>

#include "rtx/rtx_occ_rdma.hpp"
#include "rtx/rtx_occ_variants.hpp"

extern __thread RemoteHelper *remote_helper;

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
  /* register super stock level callback */
  //ro_callbacks_[TX_STOCK_LEVEL] = bind(&TpccWorker::stock_level_piece,this,_1,_2,_3,_4);
  one_shot_callbacks[TX_STOCK_LEVEL] = bind(&TpccWorker::stock_level_piece,this,_1,_2,_3,_4);
}

void TpccWorker::thread_local_init() {

  for(uint i = 0;i < server_routine + 1;++i) {

#ifdef RAD_TX
    txs_[i] = new DBRad(store_,worker_id_,rpc_,i);
#elif defined(OCC_TX)
    //txs_[i] = new DBTX(store_,worker_id_,rpc_,i);
    //remote_helper = new RemoteHelper(store_,total_partition,server_routine + 1);
    if(txs_[i]  == NULL) {
#if EM_FASST == 0
#if ONE_SIDED_READ
      new_txs_[i] = new rtx::RtxOCCR(this,store_,rpc_,current_partition,worker_id_,i,current_partition,
                                   cm,rdma_sched_,total_partition);
#else
      new_txs_[i] = new rtx::RtxOCC(this,store_,rpc_,current_partition,i,current_partition);
#endif
#else
      new_txs_[i] = new rtx::RtxOCCFast(this,store_,rpc_,current_partition,i,current_partition);
#endif
      new_txs_[i]->set_logger(new_logger_);
    }
#elif defined(SI_TX)
    txs_[i] = new DBSI(store_,worker_id_,rpc_,i);
#elif defined(FARM)
    txs_[i] = new DBFarm(cm,rdma_sched_,store_,worker_id_,rpc_,i);
#else
    fprintf(stderr,"No transaction layer used!\n");
    assert(false);
#endif
  }
  //routine_1_tx_ = txs_[1]; // used for report
  /* init local tx so that it is not a null value */
  tx_ = txs_[cor_id_];
}

txn_result_t TpccWorker::txn_new_order(yield_func_t &yield) {

  tx_->begin(db_logger_);
  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint districtID = RandomNumber(random_generator[cor_id_], 1, 10);
  //const uint districtID = 1;
  const uint customerID = GetCustomerId(random_generator[cor_id_]);
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

#define MAX_ITEM 15
  const uint numItems = RandomNumber(random_generator[cor_id_], 5, MAX_ITEM);
  //    uint itemIDs[MAX_ITEM], supplierWarehouseIDs[MAX_ITEM], orderQuantities[MAX_ITEM];
  //    float i_prices[MAX_ITEM]; //to buffer the results of the 1 hop
  bool allLocal = true;
  uint64_t remote_stocks[MAX_ITEM];
  int remote_item_ids[MAX_ITEM],local_stocks[MAX_ITEM],local_item_ids[MAX_ITEM];
  uint local_supplies[MAX_ITEM],remote_supplies[MAX_ITEM];

  int num_remote_stocks(0),num_local_stocks(0);
  std::set<uint64_t> stock_set;//remove identity stock ids
  for (uint i = 0; i < numItems; i++) {
    bool conflict = false;
    int item_id = GetItemId(random_generator[cor_id_]);
    if (NumWarehouses() == 1 ||
        RandomNumber(random_generator[cor_id_], 1, 100) > g_new_order_remote_item_pct) {
      uint supplier_warehouse_id = warehouse_id;
      uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
      if(stock_set.find(s_key)!=stock_set.end()) {
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      uint64_t s_key;
      uint supplier_warehouse_id;
      do {
        supplier_warehouse_id = RandomNumber(random_generator[cor_id_], 1, NumWarehouses());
        s_key = makeStockKey(supplier_warehouse_id, item_id);
        if(stock_set.find(s_key)!=stock_set.end()){
          conflict = true;
        } else {
          stock_set.insert(s_key);
        }
      } while (supplier_warehouse_id == warehouse_id);
      allLocal =false;

      if(conflict){
        i--;
        continue;
      }
      /* if possible, add remote stock to remote stocks */
      if(WarehouseToPartition(supplier_warehouse_id) != current_partition) {
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        /* First add remote read requests */
#if FASST == 0

        tx_->add_to_remote_set(STOC,s_key,WarehouseToPartition(supplier_warehouse_id));

#else
        tx_->remoteset->add(REQ_READ_LOCK,WarehouseToPartition(supplier_warehouse_id),STOC,s_key);
#endif
        remote_item_ids[num_remote_stocks++] = item_id;
      } else {
        local_item_ids[num_local_stocks] = item_id;
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }

  int num_servers(0);
  if(num_remote_stocks > 0) {
    num_servers = tx_->do_remote_reads();
  }
  uint64_t *c_value, *w_value;
  tx_->get(CUST,c_key, (char **)(&c_value),sizeof(customer::value));
  tx_->get(WARE,warehouse_id,(char **)(&w_value),sizeof(warehouse::value));

  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

  district::value *d_value;
  auto d_seq = tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));
  checker::SanityCheckDistrict(d_value);

  const uint64_t my_next_o_id = d_value->d_next_o_id;

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;

  tx_->insert(NEWO,no_key, (char *)(&v_no),sizeof(new_order::value));
  d_value->d_next_o_id ++;
  tx_->write(DIST,d_key,(char *)d_value,sizeof(district::value));

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  tx_->insert(ORDE,o_key,(char *)(&v_oo),sizeof(oorder::value));

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;

  tx_->insert(ORDER_INDEX,o_sec,(char *)array_dummy,sizeof(uint64_t) + sizeof(uint64_t));

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {

    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = local_stocks[ol_number  - 1];

    stock::value *s_value;
    tx_->get(STOC,s_key,(char **)(&s_value),sizeof(stock::value));

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;
    tx_->write(STOC,s_key,(char *)s_value,sizeof(stock::value));

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }
#if 1
  bool ret(true);
  if(num_remote_stocks > 0) {
    indirect_yield(yield); // yield to get the result

#if FASST == 0
    tx_->get_remote_results(num_servers); //num_servers?
    //tx_->get_remote_results(num_remote_stocks);
#else
    ret = tx_->remoteset->get_results_readlock(num_servers);
#endif
  }
#endif

  /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {
    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = remote_stocks[i];

    stock::value *s_value;
    uint64_t seq = tx_->get_cached(STOC,s_key,(char **)(&s_value));

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
    tx_->remote_write(i,(char *)s_value,sizeof(stock::value));

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }

#if FASST == 1
  // check the lock results
  if(!ret) {
    tx_->remoteset->release_remote(); // release the remote lock
    tx_->remoteset->clear_for_reads();
    return txn_result_t(false,0);     // lock check failed, abort
  }
#endif

  bool res = tx_->end(yield);
  //tx_->remoteset->update_read_buf();
  //tx_->remoteset->update_write_buf();


#if 0 // sanity checks
  if(res == true && current_partition == 0) {
    // sanity checks
    district::value *d_value;
    auto d_seq = tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));
    assert(d_value->d_next_o_id == my_next_o_id + 1);
  }
#endif
  return txn_result_t(res,0);
}

txn_result_t TpccWorker::txn_payment(yield_func_t &yield) {

  tx_->begin(db_logger_);

  uint warehouse_id;
  uint districtID;

  uint customerDistrictID, customerWarehouseID;
  bool allLocal = true;
  uint64_t d_key;
#if 1
  //here is the trick
  customerDistrictID = RandomNumber(random_generator[cor_id_], 1, NumDistrictsPerWarehouse());
  customerWarehouseID = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);

  if (likely(NumWarehouses() == 1 ||
             RandomNumber(random_generator[cor_id_], 1, 100) <= 85)) {
    districtID = customerDistrictID;
    warehouse_id = customerWarehouseID;
    d_key = makeDistrictKey(warehouse_id, districtID);
  } else {
    warehouse_id = RandomNumber(random_generator[cor_id_],1,NumWarehouses());
    districtID   = RandomNumber(random_generator[cor_id_], 1, NumDistrictsPerWarehouse());
    d_key = makeDistrictKey(warehouse_id, districtID);

    if(WarehouseToPartition(warehouse_id) != current_partition) {
      allLocal = false;
      int pid = WarehouseToPartition(warehouse_id);
      tx_->add_to_remote_set(WARE,warehouse_id,pid);
      tx_->add_to_remote_set(DIST,d_key,pid);
    }
  }

#else
  /* here is the request generation without using DrTM's transform trick, does not work for
     distributed case now */
  districtID = RandomNumber(random_generator[cor_id_],1,NumDistrictsPerWarehouse());
  warehouse_id = PickWarehouseId(random_generator[cor_id_],warehouse_id_start_,warehouse_id_end_);

  if(likely(NumWarehouses() == 1 || RandomNumber(random_generator[cor_id_],1,100)) <= 85) {
    customerWarehouseID = warehouse_id;
    customerDistrictID  = districtID;
  } else {
    /* remote cases */
    customerDistrictID = RandomNumber(random_generator[cor_id_],1,NumDistrictsPerWarehouse());
    do  {
      customerWarehouseID = PickWarehouseId(random_generator[cor_id_],warehouse_id_start_,warehouse_id_end_);

    } while(customerWarehouseID == warehouse_id);
  }
  d_key = makeDistrictKey(warehouse_id, districtID);
#endif
  const uint64_t addAmount = (uint64_t)(RandomNumber(random_generator[cor_id_],100,500000));
  const float paymentAmount = (float) (addAmount / 100.0);

  const uint32_t ts = GetCurrentTimeMillis();

  /* transaction payment's execution starts */
  if(!allLocal) {
    int num_servers = tx_->do_remote_reads();
    indirect_yield(yield);
    tx_->get_remote_results(num_servers);

  } else {         }

  uint64_t c_key;
  customer::value *v_c;
#if 1
  if (RandomNumber(random_generator[cor_id_], 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, random_generator[cor_id_]);

    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    std::string clast;
    clast.assign((const char *) lastname_buf, 16);
    uint64_t c_start = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, zeros);
    uint64_t c_end   = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, ones);

#ifdef  RAD_TX
    RadIterator  iter((DBRad *)tx_, CUST_INDEX, false);
#elif defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_, CUST_INDEX,false);
#elif defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,CUST_INDEX,false);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,CUST_INDEX,false);
#endif

    iter.Seek(c_start);
    uint64_t c_keys[100];
    int j = 0;

    while (iter.Valid()) {

      if (compareCustomerIndex(iter.Key(), c_end)) {

        uint64_t *prikeys = (uint64_t *)((char *)(iter.Value())  + META_LENGTH);
        int num = prikeys[0];
        assert(num < 16);
        for (int i = 1; i <= num; i++) {
          c_keys[j] = prikeys[i];
          assert(prikeys[i] != 0);
          j++;
        }
        if (j >= 100) {
          printf("P Array Full\n");
          exit(0);
        }
      }
      else {
        break;
      }
      iter.Next();
    }
    j = (j+1)/2 - 1;
    c_key = c_keys[j];
    uint64_t res = tx_->get(CUST, c_key, (char **)(&v_c),sizeof(customer::value));
    if(unlikely(res == 1)) {

      uint64_t upper = (c_key >> 32);
      fprintf(stderr,"customer not found!! %lu\n",c_key);

      int ware = upper / 10;
      int d = upper % 10;
      fprintf(stdout,"ware %d, dist %d, cust ware %d, dist %d\n",ware,d,
              customerWarehouseID,customerDistrictID);
      assert(false);
    }

  } else {
    // cust by ID
    const uint customerID = GetCustomerId(random_generator[cor_id_]);
    c_key = makeCustomerKey(customerWarehouseID,customerDistrictID,customerID);

    uint64_t res = tx_->get(CUST, c_key, (char **)(&v_c),sizeof(customer::value));
    if(unlikely(res == 1)) {
      fprintf(stderr,"customer not found!! %d %d %d\n",customerWarehouseID,customerDistrictID,
              customerID);
      exit(-1);
    }
  }

  v_c->c_balance -= paymentAmount;
  v_c->c_ytd_payment += paymentAmount;
  v_c->c_payment_cnt += 1;

  if (strncmp(v_c->c_credit.data(), "BC", 2) == 0) {
    char buf[501];
    int d_id = static_cast<int32_t>(c_key >> 32) % 10;
    if (d_id == 0) d_id = 10;
    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
                     static_cast<int32_t>(c_key << 32 >> 32),
                     d_id,
                     (static_cast<int32_t>(c_key >> 32) - d_id)/10,
                     districtID,
                     warehouse_id,
                     paymentAmount,
                     v_c->c_data.c_str());
    v_c->c_data.resize_junk(
        std::min(static_cast<size_t>(n), v_c->c_data.max_size()));
    NDB_MEMCPY((void *) v_c->c_data.data(), &buf[0], v_c->c_data.size());
  }
  tx_->write(CUST,c_key,(char *)v_c,sizeof(customer::value));
#endif

  int d_id = static_cast<int32_t>(c_key >> 32) % 10;
  if (d_id == 0) d_id = 10;
  uint64_t h_key = makeHistoryKey(static_cast<int32_t>(c_key << 32 >> 32),
                                  d_id, (static_cast<int32_t>(c_key >> 32)-d_id) / 10,
                                  districtID, warehouse_id);
  /* a little re-ordering, so that we can use coroutine to hide the remote fetch latency */
  warehouse::value *v_w;
  district::value *v_d;
  if(!allLocal) {
#if 1
    uint64_t seq = tx_->get_cached(WARE,warehouse_id,(char **)(&v_w));
    assert(seq != 0);
    seq = tx_->get_cached(DIST,d_key,(char **)(&v_d));
    assert(seq != 0);
    //checker::SanityCheckDistrict(v_d);
    v_w->w_ytd += paymentAmount;
    v_d->d_ytd += paymentAmount;

    tx_->remote_write(0,(char *)v_w,sizeof(warehouse::value));
    tx_->remote_write(1,(char *)v_d,sizeof(district::value));
    //assert(seq != 0);
#endif
  } else {
#if 1
    tx_->get(WARE,warehouse_id,(char **)(&v_w),sizeof(warehouse::value));
    tx_->write(WARE,warehouse_id,(char *)v_w,sizeof(warehouse::value));
    assert(d_key != 0);
    tx_->get(DIST,d_key,(char **)(&v_d),sizeof(district::value));
    tx_->write(DIST,d_key,(char *)v_d,sizeof(district::value));

    v_w->w_ytd += paymentAmount;
    v_d->d_ytd += paymentAmount;
#endif
  }
#if 1
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s",
                   v_w->w_name.c_str(),
                   v_d->d_name.c_str());
  v_h.h_data.resize_junk(std::min(static_cast<size_t>(n), v_h.h_data.max_size() - 1));

  const size_t history_sz = Size(v_h);
  ssize_t ret = 0;
  ret += history_sz;

  tx_->insert(HIST,h_key,(char *)(&v_h),sizeof(history::value));
#endif
  bool res = tx_->end(yield);
  return txn_result_t(res,73);
  //        return txn_result_t(true,73);
}

txn_result_t TpccWorker::txn_delivery(yield_func_t &yield) {

  static __thread int counter = 0;

  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint o_carrier_id = RandomNumber(random_generator[cor_id_], 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  tx_->begin(db_logger_);

  uint res = 0;

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {

    int32_t no_o_id = 1;
    uint64_t *no_value;
    int64_t start = makeNewOrderKey(warehouse_id, d, last_no_o_ids_[warehouse_id -1][d - 1]);
    int64_t end = makeNewOrderKey(warehouse_id, d, std::numeric_limits<int32_t>::max());
    int64_t no_key  = -1;

#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,NEWO);
#elif  defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,NEWO);
#elif  defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,NEWO);
#elif  defined(SI_TX)
    SIIterator iter((DBSI *)tx_,NEWO);
#endif
    iter.Seek(start);
    if (iter.Valid()) {

      no_key = iter.Key();

      if (no_key <= end) {
        no_o_id = static_cast<int32_t>(no_key << 32 >> 32);
        //fprintf(stderr,"delete %d %d, no key %lu\n",warehouse_id - 1,d-1, no_o_id);
        tx_->delete_(NEWO, no_key);
        // update here
        //stderr,"Last %lu, me %lu\n",last_no_o_ids_[warehouse_id - 1][d-1],no_o_id);
        last_no_o_ids_[warehouse_id - 1][d-1] = no_o_id + 1;
      }
      else {
        no_key = -1;
      }
    } else {
      //fprintf(stderr,"last no order id %lu, ware %d, d %d\n",last_no_o_ids_[warehouse_id - 1][d-1],warehouse_id,d);
      //assert(false);
    }
    if (no_key == -1) {
      iter.SeekToFirst();
      // Count as user inited abort
      tx_->abort();
      return txn_result_t(true, 0);
      // skip this one
      //continue;
    }

    uint64_t o_key = makeOrderKey(warehouse_id, d, no_o_id);
    oorder::value *o_value = NULL;
    int seq = tx_->get(ORDE, o_key, (char **)(&o_value),sizeof(oorder::value));
    if(unlikely(seq == 1)) {
      continue;
    }

    float sum_ol_amount = 0;

#ifdef RAD_TX
    RadIterator iter1((DBRad *)tx_,ORLI);
#elif  defined(OCC_TX)
    DBTXIterator iter1((DBTX *)tx_,ORLI);
#elif  defined(FARM)
    DBFarmIterator iter1((DBFarm *)tx_,ORLI);
#elif  defined(SI_TX)
    SIIterator iter1((DBSI *)tx_,ORLI);
#endif

    int ol_count;

    start = makeOrderLineKey(warehouse_id, d, no_o_id, 1);
    iter1.Seek(start);
    end = makeOrderLineKey(warehouse_id, d, no_o_id, 15);
    //      int ol_c(0);

    while (iter1.Valid()) {
      int64_t ol_key = iter1.Key();
      if (ol_key > end || ol_count >= 15) {
        break;
      } else {

      }

      order_line::value *v_ol;
      seq = tx_->get(ORLI,ol_key,(char **)(&v_ol),sizeof(order_line::value));
      assert(seq != 1);
      sum_ol_amount += v_ol->ol_amount;
      ol_count += 1;

      v_ol->ol_delivery_d = ts;
      tx_->write(ORLI,ol_key,(char *)(v_ol),sizeof(order_line::value));
      iter1.Next();
    }

    o_value->o_carrier_id = o_carrier_id;
    tx_->write(ORDE,o_key,(char *)o_value,sizeof(oorder::value));

    const uint c_id = o_value->o_c_id;

    const float ol_total = sum_ol_amount;
    uint64_t c_key = makeCustomerKey(warehouse_id, d, c_id);
    customer::value *v_c;

    seq = tx_->get(CUST,c_key,((char **)(&v_c)),sizeof(customer::value));
    assert(seq != 1);
    checker::SanityCheckCustomer(v_c);
    v_c->c_balance += ol_total;
    tx_->write(CUST,c_key,(char *)v_c,sizeof(customer::value));
    /* end iterate district per-warehouse */
  }
  bool ret = tx_->end(yield);
  return txn_result_t(ret,res);
}


txn_result_t TpccWorker::txn_stock_level(yield_func_t &yield) {

  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint threshold = RandomNumber(random_generator[cor_id_], 10, 20);
  const uint districtID = RandomNumber(random_generator[cor_id_], 1, NumDistrictsPerWarehouse());

  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

  tx_->local_ro_begin();
#ifdef RC
  district::value *d_p;
  tx_->get(DIST,d_key,(char **)(&d_p),sizeof(district::value));
  tx_->write(DIST,d_key,NULL,0);
  uint64_t cur_next_o_id = d_p->d_next_o_id;
#else
  district::value v_d ;
  tx_->get_ro(DIST,d_key,(char *)(&v_d),yield);
  uint64_t cur_next_o_id = v_d.d_next_o_id;
#endif

  const int32_t lower = cur_next_o_id >= STOCK_LEVEL_ORDER_COUNT ? (cur_next_o_id - STOCK_LEVEL_ORDER_COUNT) : 0;
  uint64_t start = makeOrderLineKey(warehouse_id, districtID, lower, 0);
  uint64_t end   = makeOrderLineKey(warehouse_id, districtID, cur_next_o_id, 0);

  std::vector<int32_t> ol_i_ids;
  std::vector<int32_t> s_i_ids;

#ifdef RAD_TX
  RadIterator  iter((DBRad *)tx_,ORLI);
#elif defined(OCC_TX)
  DBTXIterator iter((DBTX *)tx_,ORLI);
#elif defined(FARM)
  DBFarmIterator iter((DBFarm *)tx_,ORLI);
#elif defined(SI_TX)
  SIIterator iter((DBSI *)tx_,ORLI);
#endif
  iter.Seek(start);

  while (iter.Valid()) {

    uint64_t ol_key = iter.Key();

    if (ol_key >= end) break;

    //uint64_t *ol_value = (uint64_t *)((char *)(iter.Value()) + META_LENGTH);
    //      order_line::value *v_ol = (order_line::value *)ol_value;
    order_line::value v_ol;
    if(unlikely(tx_->get_ro(ORLI,ol_key,(char *)(&v_ol),yield) == 1)) {
      //fprintf(stdout,"key %lu, val %p valid %d\n",iter.Key(),iter.Value(),iter.Valid());
      //assert(false);
      goto NEXT;
    }
    {
      int64_t s_key = makeStockKey(warehouse_id, v_ol.ol_i_id);

#ifdef RC
      stock::value *s_p;
      tx_->get(STOC,s_key,(char **)(&s_p), sizeof(stock::value));
      tx_->write(STOC,s_key,NULL,0);
      if(s_p->s_quantity < int(threshold)) {
        s_i_ids.push_back(s_key);
      }

#else
      stock::value v_s;
      tx_->get_ro(STOC,s_key,(char *)(&v_s),yield);
      if(v_s.s_quantity < int(threshold)) {
        s_i_ids.push_back(s_key);
      }
#endif
    }
 NEXT:
    iter.Next();
  }
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
  return txn_result_t(ret,s_i_ids.size() + ol_i_ids.size());
#else
  return txn_result_t(true,s_i_ids.size() + ol_i_ids.size());
#endif
}

txn_result_t TpccWorker::txn_super_stock_level(yield_func_t &yield) {

  //        fprintf(stdout,"super stock level starts\n");
  uint local_warehouses[MAX_DIST];
  uint local_districts[MAX_DIST];
  int  num_local(0);
  int  num_remote(0);

  const uint threshold = RandomNumber(random_generator[cor_id_], 10, 20);

  int num_dists = MAX_DIST;
  //      int num_dists = RandomNumber(random_generator[cor_id_], 2, 2 * total_partition);

  int res(0);

  ForkSet fork_handler(rpc_,cor_id_);
  //fprintf(stdout,"fork handler init\n");
  // prepare the request payload
  struct StockLevelInput *s_header = (struct StockLevelInput *)fork_handler.do_fork(sizeof(struct StockLevelInput));

  for(uint i = 0;i < num_dists;++i) {
    uint warehouse_id = RandomNumber(random_generator[cor_id_],1,NumWarehouses());
    uint districtID = RandomNumber(random_generator[cor_id_],1,NumDistrictsPerWarehouse());
    int pid;
    if( (pid = WarehouseToPartition(warehouse_id)) == current_partition) {
      local_warehouses[num_local] = warehouse_id;
      local_districts[num_local++] = districtID;
    } else {
      /* add to remote set */
      struct StockLevelInputPayload *p = (struct StockLevelInputPayload *)
                                         fork_handler.add(pid,sizeof(struct StockLevelInputPayload));
      p->warehouse_id = warehouse_id;
      p->district_id   = districtID;
      p->pid = pid;
      num_remote += 1;
    }
  }
#ifdef SI_TX
  /* gotten timestamp */
  ((DBSI *)tx_)->get_ts_vec();
#elif defined(RAD_TX)
  /* this call will also set transaction's internal structure */
  s_header->timestamp = ((DBRad *)tx_)->get_timestamp();
#endif

  int num_server(0);
  /* fork remote */
  if(num_remote > 0) {

#ifdef SI_TX
    /* setting the timestamp */
    memcpy( &(s_header->ts_vec),((DBSI *)(tx_))->ts_buffer_, sizeof(uint64_t) * total_partition);
#endif

    /* fork */
    s_header->num = num_remote;
    s_header->threshold = threshold;
    num_server = fork_handler.fork(TX_STOCK_LEVEL);
  }
  /***************/
  //fprintf(stdout,"fork done\n");
#if 1
#ifdef OCC_TX
  /* execute local */
  //tx_->begin();
  ((DBTX *)tx_)->local_ro_begin();
#endif

  for(uint i = 0;i < num_local;++i) {

    uint64_t d_key = makeDistrictKey(local_warehouses[i],local_districts[i]);

#ifdef RC
    district::value *v_p;
    tx_->get(DIST,d_key,(char **)(&v_p),sizeof(district::value));
    tx_->write(DIST,d_key,(char *)v_p,sizeof(district::value));
    uint64_t cur_next_o_id = v_p->d_next_o_id;
#else
    district::value v_d;
    uint64_t d_seq = tx_->get_ro(DIST,d_key,(char *)(&v_d),yield);
    assert(d_seq != 0);
    uint64_t cur_next_o_id = v_d.d_next_o_id;
#endif

    //  int stock_level_count = MIN(2,STOCK_LEVEL_ORDER_COUNT - total_partition * 2);
    const int32_t lower = cur_next_o_id >= STOCK_LEVEL_ORDER_COUNT ? (cur_next_o_id - STOCK_LEVEL_ORDER_COUNT) : 0;
    uint64_t start = makeOrderLineKey(local_warehouses[i], local_districts[i], lower, 0);
    uint64_t end   = makeOrderLineKey(local_warehouses[i], local_districts[i], cur_next_o_id, 0);

#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,ORLI);
#elif  defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,ORLI);
#elif  defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,ORLI);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,ORLI);
#endif
    iter.Seek(start);

    while(iter.Valid()) {
      int64_t ol_key = iter.Key();
      if(ol_key >= end) break;
      order_line::value v_ol;
      if(unlikely(tx_->get_ro(ORLI,ol_key,(char *)(&v_ol),yield) == 1)) {
        /* if u allow SI to relax checks, then it is ok do so */
        //      fprintf(stdout,"d_key seq %lu\n",d_seq);
        //      assert(false);
        goto NEXT;
      }
      {
        int64_t s_key = makeStockKey(local_warehouses[i], v_ol.ol_i_id);
#ifdef RC
        stock::value *v_p;
        tx_->get(STOC,s_key,(char **)(&v_p),sizeof(stock::value));
        tx_->write(STOC,s_key,(char *)v_p,sizeof(stock::value));
        if(v_p->s_quantity < int(threshold)) {
          res += 1;
        }
#else
        stock::value v_s;
        tx_->get_ro(STOC,s_key,(char *)(&v_s),yield);
        if(v_s.s_quantity < int(threshold)) {
          res += 1;
        }
#endif
      }
   NEXT:
      iter.Next();
    }
  }
#endif
  if(num_remote > 0) {
    //fprintf(stdout,"send validate\n");
    /* join remote */
    indirect_yield(yield);
    //#ifdef 0
#ifdef OCC_TX
    /* send validation */
    //fork_handler.do_fork(0);
    fork_handler.reset();
    fork_handler.fork(RPC_R_VALIDATE,0);
    indirect_yield(yield);

    /* parse reply */
    int8_t *reply_p = (int8_t *)( fork_handler.reply_buf_);
    for(uint i = 0;i < num_server;++i) {
      //    fprintf(stdout,"reply val %d\n",reply_p[i]);
      if(!reply_p[i]) {
        return txn_result_t(false,0);
      }
    }
#else
    /* dbrad and dbsi do not need a secend round validation */
#endif
    /***************/
  }

#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
  return txn_result_t(ret,res);
#else
  return txn_result_t(true,res);
#endif
}

txn_result_t TpccWorker::txn_order_status(yield_func_t &yield) {

  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint districtID = RandomNumber(random_generator[cor_id_], 1, NumDistrictsPerWarehouse());
  //const uint districtID = 1;
  uint64_t c_key;
  customer::value v_c;

  tx_->local_ro_begin();

  if (RandomNumber(random_generator[cor_id_], 1, 100) <= 60) {
    //1.1 cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, random_generator[cor_id_]);

    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    std::string clast;
    clast.assign((const char *) lastname_buf, 16);
    uint64_t c_start = makeCustomerIndex(warehouse_id, districtID, clast, zeros);
    uint64_t c_end = makeCustomerIndex(warehouse_id, districtID, clast, ones);

#ifdef RAD_TX
    RadIterator  citer((DBRad *)tx_,CUST_INDEX ,false);
#elif  defined(OCC_TX)
    DBTXIterator citer((DBTX *)tx_, CUST_INDEX,false);
#elif  defined(FARM)
    DBFarmIterator citer((DBFarm *)tx_,CUST_INDEX,false);
#elif  defined(SI_TX)
    SIIterator citer((DBSI *)tx_,CUST_INDEX,false);
#endif

    citer.Seek(c_start);

    uint64_t *c_values[100];
    uint64_t c_keys[100];
    int j = 0;
    while (citer.Valid()) {

      if (compareCustomerIndex(citer.Key(), c_end)) {

        uint64_t *prikeys = (uint64_t *)((char *)(citer.Value()) + META_LENGTH);

        int num = prikeys[0];

        for (int i=1; i <= num; i++) {
          c_keys[j] = prikeys[i];
          j++;
        }

        if (j >= 100) {
          printf("OS Array Full\n");
          exit(0);
        }
      }
      else {
        break;
      }
      citer.Next();
    }

    j = (j+1)/2 - 1;
    c_key = c_keys[j];
    //      tx_->get(CUST,c_key,(char **)(&v_c),sizeof(customer::value));
    tx_->get_ro(CUST,c_key,(char *)(&v_c),yield);
  } else {
    //1.2 cust by ID
    const uint customerID = GetCustomerId(random_generator[cor_id_]);
    c_key = makeCustomerKey(warehouse_id,districtID,customerID);
#ifdef RC
    customer::value *c_p;
    tx_->get(CUST,c_key,(char **)(&c_p),sizeof(customer::value));
    tx_->write(CUST,c_key,NULL,sizeof(customer::value));
#else
    tx_->get_ro(CUST,c_key,(char *)(&v_c),yield);
#endif
  }

  //STEP 2. read record from ORDER
  int32_t o_id = -1;
  int o_ol_cnt;
#ifdef RAD_TX
  RadIterator  iter((DBRad *)tx_,ORDER_INDEX);
#elif defined(OCC_TX)
  DBTXIterator iter((DBTX *)tx_,ORDER_INDEX);
#elif defined(FARM)
  DBFarmIterator iter((DBFarm *)tx_,ORDER_INDEX);
#elif defined(SI_TX)
  SIIterator iter((DBSI *)tx_,ORDER_INDEX);
#endif

  uint64_t start = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 10000000+ 1);
  uint64_t end = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 1);

  iter.Seek(start);
  if(iter.Valid())
    iter.Prev();
  else printf("ERROR: SeekOut!!!\n");

  if (iter.Valid() && iter.Key() >= end) {

    uint64_t *prikeys = (uint64_t *)((char *)(iter.Value()) + META_LENGTH);
    o_id = static_cast<int32_t>(prikeys[1] << 32 >> 32);

    oorder::value *ol_p;
#ifdef RC
    uint64_t o_seq = tx_->get(ORDE, prikeys[1],(char **)(&ol_p),sizeof(oorder::value));
    tx_->write(ORDE,prikeys[1],NULL,0);
    o_ol_cnt = ol_p->o_ol_cnt;
#else
    oorder::value v_ol;
    uint64_t od_seq = tx_->get_ro(ORDE,prikeys[1],(char *)(&v_ol),yield);
    checker::SanityCheckOOrder(&v_ol);
    o_ol_cnt = v_ol.o_ol_cnt;
#endif
    if(od_seq == 0) {
#ifdef OCC
      assert(false);
#endif
      goto END;
    }
    //    assert(false);

    //STEP 3. read record from ORDERLINE
    if (o_id != -1) {

      for (int32_t line_number = 1; line_number <= o_ol_cnt; ++line_number) {
        uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, o_id, line_number);

#ifdef      RC
        order_line::value *ol_p;
        tx_->get(ORLI,ol_key,(char **)(&ol_p),sizeof(order_line::value));
        tx_->write(ORLI,ol_key,NULL,0);
#else
        order_line::value ol_val;
        uint64_t o_seq = tx_->get_ro(ORLI,ol_key,(char *)(&ol_val),yield);
#endif
      }
    }
    else {
      printf("ERROR: Customer %d No order Found\n", static_cast<int32_t>(c_key << 32 >> 32));
    }
  }
END:
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
  return txn_result_t(ret,o_ol_cnt);
#else
  return txn_result_t(true,o_ol_cnt);
#endif
}

txn_result_t TpccWorker::txn_micro(yield_func_t &yield) {

  tx_->begin();

  std::set<uint64_t> d_set;
  int write_num = (int)(g_mico_dist_num * ((double)g_new_order_remote_item_pct / 100));
  //      fprintf(stdout,"write item %d\n",write_num);

  uint64_t d_keys[MICRO_DIST_NUM];

  for(uint i = 0; i < g_mico_dist_num;++i) {
 retry:
    const uint warehouse_id = PickWarehouseId(random_generator[cor_id_],1,NumWarehouses());
    const uint districtID = RandomNumber(random_generator[cor_id_],1,10);
    uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
    if(unlikely(d_set.find(d_key) != d_set.end()))
      goto retry;
    d_set.insert(d_key);
    d_keys[i] = d_key;
  }

  uint64_t ret;
  for(uint i = 0;i < write_num;++i) {
    /* needs write */
    district::value *d_value;
    tx_->get(DIST,d_keys[i], (char **)(&d_value), sizeof(district::value));
    d_value->d_next_o_id += 1;
    tx_->write(DIST,d_keys[i],(char *)d_value,sizeof(district::value));
  }
  for(uint i = write_num;i < g_mico_dist_num;++i) {
    district::value *d_value;
    tx_->get(DIST,d_keys[i], (char **)(&d_value), sizeof(district::value));
    ret += d_value->d_next_o_id;
  }
  //      assert(false);
  bool res = tx_->end(yield);
  return txn_result_t(res,ret);
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

void TpccWorker::check_consistency() {

  fprintf(stdout,"[TPCC]: check consistency.\n");
  return;
  {
    // check the new order insert status
    tx_->begin();
#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,NEWO);
#elif  defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,NEWO);
#elif  defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,NEWO);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,NEWO);
#endif

    for(uint w = warehouse_id_start_; w < warehouse_id_end_;++w) {
      for(uint d = 1; d <= NumDistrictsPerWarehouse();++d) {
        //int64_t start = makeNewOrderKey(warehouse_id, d, 0);
        int64_t end = makeNewOrderKey(w, d, std::numeric_limits<int32_t>::max());
        iter.Seek(end);
        iter.Prev();
        assert(iter.Valid());
        // parse the no id from the key
        uint64_t key = iter.Key();
        uint64_t d_id = key & 0xffffffff;
        uint64_t d_key = makeDistrictKey(w, d);
        district::value *d_value;
        tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));
        checker::SanityCheckDistrict(d_value);

        const uint64_t my_next_o_id = d_value->d_next_o_id;

        fprintf(stdout,"[TPCC] ware %d, dis %d, id %lu,next entry in district %lu\n",w,d,d_id,my_next_o_id);
        fprintf(stdout,"[TPCC] last processed %lu\n",last_no_o_ids_[w-1][d-1]);

        assert(((key >> 32) & 0xffffffff) == w * 10 + d);
      }
    }
    // end check new order
  }

#if 0
  warehouse::value *v_w;
  tx_->begin();
  tx_->get(WARE,warehouse_id_start_, (char **)(&v_w),sizeof(warehouse::value));
  int64_t all_dist_ytd(0);

  for(uint i = 1;i <= NumDistrictsPerWarehouse();++i) {
    district::value *v_d;
    uint64_t d_key = makeDistrictKey(warehouse_id_start_,i);
    tx_->get(DIST,d_key,(char **)(&v_d),sizeof(district::value));
    all_dist_ytd += v_d->d_ytd;
  }
  fprintf(stdout,"w ytd %lu d total %lu %d\n",v_w->w_ytd,all_dist_ytd,v_w->w_ytd == all_dist_ytd);
#endif
}

// naive version of TX new order and payment  ///////////////////////////

txn_result_t TpccWorker::txn_new_order_naive(yield_func_t &yield) {

  tx_->begin(db_logger_);

  // generate the req parameters ////////////////////////////////////////
  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint districtID = RandomNumber(random_generator[cor_id_], 1, 10);
  //const uint districtID = 1;
  const uint customerID = GetCustomerId(random_generator[cor_id_]);
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

  bool allLocal = true;
  std::set<uint64_t> stock_set; //remove identity stock ids;

  // local buffer used store stocks
  uint64_t remote_stocks[MAX_ITEM];
  int remote_item_ids[MAX_ITEM],local_stocks[MAX_ITEM],local_item_ids[MAX_ITEM];
  uint local_supplies[MAX_ITEM],remote_supplies[MAX_ITEM];

  int num_remote_stocks(0),num_local_stocks(0);

#define MAX_ITEM 15
  const uint numItems = RandomNumber(random_generator[cor_id_], 5, MAX_ITEM);

  for (uint i = 0; i < numItems; i++) {
    bool conflict = false;
    int item_id = GetItemId(random_generator[cor_id_]);
    if (NumWarehouses() == 1 ||
        RandomNumber(random_generator[cor_id_], 1, 100) > g_new_order_remote_item_pct) {
      // locla stock case
      uint supplier_warehouse_id = warehouse_id;
      uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
      if(stock_set.find(s_key)!=stock_set.end()) {
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      // remote stock case
      uint64_t s_key;
      uint supplier_warehouse_id;
      do {
        supplier_warehouse_id = RandomNumber(random_generator[cor_id_], 1, NumWarehouses());
        s_key = makeStockKey(supplier_warehouse_id, item_id);
        if(stock_set.find(s_key)!=stock_set.end()){
          conflict = true;
        } else {
          stock_set.insert(s_key);
        }
      } while (supplier_warehouse_id == warehouse_id);
      allLocal =false;

      if(conflict){
        i--;
        continue;
      }
      /* if possible, add remote stock to remote stocks */
      if(WarehouseToPartition(supplier_warehouse_id) != current_partition) {
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        remote_item_ids[num_remote_stocks++] = item_id;
        // naive version will not do remote reads here
      } else {
        local_item_ids[num_local_stocks] = item_id;
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }

  // Execution phase ////////////////////////////////////////////////////

  uint64_t *c_value,*w_value;
  tx_->get(CUST,c_key,(char **)(&c_value),sizeof(customer::value));
  tx_->get(WARE,warehouse_id,(char **)(&w_value),sizeof(warehouse::value));

  uint64_t d_key = makeDistrictKey(warehouse_id,districtID);

  district::value *d_value;
  auto d_seq = tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));

  const auto my_next_o_id = d_value->d_next_o_id;

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;

  tx_->insert(NEWO,no_key, (char *)(&v_no),sizeof(new_order::value));
  d_value->d_next_o_id ++;
  tx_->write(DIST,d_key,(char *)d_value,sizeof(district::value));

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  tx_->insert(ORDE,o_key,(char *)(&v_oo),sizeof(oorder::value));

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;

  tx_->insert(ORDER_INDEX,o_sec,(char *)array_dummy,sizeof(uint64_t) + sizeof(uint64_t));

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {

    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = local_stocks[ol_number  - 1];

    stock::value *s_value;
    tx_->get(STOC,s_key,(char **)(&s_value),sizeof(stock::value));

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;
    tx_->write(STOC,s_key,(char *)s_value,sizeof(stock::value));

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }

  /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {
    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = remote_stocks[i];

    stock::value *s_value;

    // fetch remote objects
#ifdef FARM // one-sided read
    int idx = tx_->add_to_remote_set(STOC,s_key,WarehouseToPartition(stockKeyToWare(s_key)),yield);
#else
    int idx = tx_->add_to_remote_set_imm(STOC,s_key,WarehouseToPartition(stockKeyToWare(s_key)));
#endif

    // yield for result
    indirect_yield(yield);
    // parse the result
    tx_->get_imm_res(idx,(char **)(&s_value),sizeof(stock::value));
#if 1
    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
#endif
    // prepare lock and write records
    tx_->remote_write(i,(char *)s_value,sizeof(stock::value));

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }

  // Commit phase ////////////////////////////////////////////////////
#if !TX_ONLY_EXE
  bool res = tx_->end(yield);
#else
  bool res = true;
#ifndef FARM
  //tx_->remoteset->update_read_buf();
  tx_->remoteset->update_write_buf();
#endif
#endif

  //fprintf(stdout,"commit\n");
  //sleep(1);
  return txn_result_t(res,10);
}

txn_result_t TpccWorker::txn_payment_naive(yield_func_t &yield) {

  warehouse::value *v_w;
  district::value  *v_d;

  tx_->begin(db_logger_);

  // generate user request //////////////////////////////////////////////
  uint warehouse_id;
  uint districtID;

  uint customerDistrictID, customerWarehouseID;
  bool allLocal = true;
  uint64_t d_key;

  //here is the trick
  customerDistrictID = RandomNumber(random_generator[cor_id_], 1, NumDistrictsPerWarehouse());
  customerWarehouseID = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);

  if (likely(NumWarehouses() == 1 ||
             RandomNumber(random_generator[cor_id_], 1, 100) <= 85)) {
    districtID = customerDistrictID;
    warehouse_id = customerWarehouseID;
    d_key = makeDistrictKey(warehouse_id, districtID);
  } else {
    warehouse_id = RandomNumber(random_generator[cor_id_],1,NumWarehouses());
    districtID   = RandomNumber(random_generator[cor_id_], 1, NumDistrictsPerWarehouse());
    d_key = makeDistrictKey(warehouse_id, districtID);

    if(WarehouseToPartition(warehouse_id) != current_partition) {
      allLocal = false;
      int pid = WarehouseToPartition(warehouse_id);

      // get the warehouse
      int idx = tx_->add_to_remote_set_imm(WARE,warehouse_id,pid);
      indirect_yield(yield);
      tx_->get_imm_res(idx,(char **)(&v_w),sizeof(warehouse::value));

      // get the district
      idx = tx_->add_to_remote_set_imm(DIST,d_key,pid);
      indirect_yield(yield);
      tx_->get_imm_res(idx,(char **)(&v_d),sizeof(district::value));
    }
  }

  const uint64_t addAmount = (uint64_t)(RandomNumber(random_generator[cor_id_],100,500000));
  const float paymentAmount = (float) (addAmount / 100.0);

  const uint32_t ts = GetCurrentTimeMillis();

  // naive version does not need do remote reads

  uint64_t c_key;
  customer::value *v_c;
#if 1
  if (RandomNumber(random_generator[cor_id_], 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, random_generator[cor_id_]);

    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    std::string clast;
    clast.assign((const char *) lastname_buf, 16);
    uint64_t c_start = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, zeros);
    uint64_t c_end   = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, ones);

#ifdef  RAD_TX
    RadIterator  iter((DBRad *)tx_, CUST_INDEX, false);
#elif defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_, CUST_INDEX,false);
#elif defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,CUST_INDEX,false);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,CUST_INDEX,false);
#endif

    iter.Seek(c_start);
    uint64_t c_keys[100];
    int j = 0;

    while (iter.Valid()) {

      if (compareCustomerIndex(iter.Key(), c_end)) {

        uint64_t *prikeys = (uint64_t *)((char *)(iter.Value())  + META_LENGTH);
        int num = prikeys[0];
        assert(num < 16);
        for (int i = 1; i <= num; i++) {
          c_keys[j] = prikeys[i];
          assert(prikeys[i] != 0);
          j++;
        }
        if (j >= 100) {
          printf("P Array Full\n");
          exit(0);
        }
      }
      else {
        break;
      }
      iter.Next();
    }
    j = (j+1)/2 - 1;
    c_key = c_keys[j];
    uint64_t res = tx_->get(CUST, c_key, (char **)(&v_c),sizeof(customer::value));
    if(unlikely(res == 1)) {

      uint64_t upper = (c_key >> 32);
      fprintf(stderr,"customer not found!! %lu\n",c_key);

      int ware = upper / 10;
      int d = upper % 10;
      fprintf(stdout,"ware %d, dist %d, cust ware %d, dist %d\n",ware,d,
              customerWarehouseID,customerDistrictID);
      assert(false);
    }

  } else {
    // cust by ID
    const uint customerID = GetCustomerId(random_generator[cor_id_]);
    c_key = makeCustomerKey(customerWarehouseID,customerDistrictID,customerID);

    uint64_t res = tx_->get(CUST, c_key, (char **)(&v_c),sizeof(customer::value));
    if(unlikely(res == 1)) {
      fprintf(stderr,"customer not found!! %d %d %d\n",customerWarehouseID,customerDistrictID,
              customerID);
      exit(-1);
    }
  }

  v_c->c_balance -= paymentAmount;
  v_c->c_ytd_payment += paymentAmount;
  v_c->c_payment_cnt += 1;

  if (strncmp(v_c->c_credit.data(), "BC", 2) == 0) {
    char buf[501];
    int d_id = static_cast<int32_t>(c_key >> 32) % 10;
    if (d_id == 0) d_id = 10;
    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
                     static_cast<int32_t>(c_key << 32 >> 32),
                     d_id,
                     (static_cast<int32_t>(c_key >> 32) - d_id)/10,
                     districtID,
                     warehouse_id,
                     paymentAmount,
                     v_c->c_data.c_str());
    v_c->c_data.resize_junk(
        std::min(static_cast<size_t>(n), v_c->c_data.max_size()));
    NDB_MEMCPY((void *) v_c->c_data.data(), &buf[0], v_c->c_data.size());
  }
  tx_->write(CUST,c_key,(char *)v_c,sizeof(customer::value));
#endif

  int d_id = static_cast<int32_t>(c_key >> 32) % 10;
  if (d_id == 0) d_id = 10;
  uint64_t h_key = makeHistoryKey(static_cast<int32_t>(c_key << 32 >> 32),
                                  d_id, (static_cast<int32_t>(c_key >> 32)-d_id) / 10,
                                  districtID, warehouse_id);
  /* a little re-ordering, so that we can use coroutine to hide the remote fetch latency */
  if(!allLocal) {
#if 1
    v_w->w_ytd += paymentAmount;
    v_d->d_ytd += paymentAmount;

    tx_->remote_write(0,(char *)v_w,sizeof(warehouse::value));
    tx_->remote_write(1,(char *)v_d,sizeof(district::value));
    //assert(seq != 0);
#endif
  } else {
#if 1
    tx_->get(WARE,warehouse_id,(char **)(&v_w),sizeof(warehouse::value));
    tx_->write(WARE,warehouse_id,(char *)v_w,sizeof(warehouse::value));
    assert(d_key != 0);
    tx_->get(DIST,d_key,(char **)(&v_d),sizeof(district::value));
    tx_->write(DIST,d_key,(char *)v_d,sizeof(district::value));

    v_w->w_ytd += paymentAmount;
    v_d->d_ytd += paymentAmount;
#endif
  }
#if 1
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s",
                   v_w->w_name.c_str(),
                   v_d->d_name.c_str());
  v_h.h_data.resize_junk(std::min(static_cast<size_t>(n), v_h.h_data.max_size() - 1));

  const size_t history_sz = Size(v_h);
  ssize_t ret = 0;
  ret += history_sz;

  tx_->insert(HIST,h_key,(char *)(&v_h),sizeof(history::value));
#endif


  bool res = tx_->end(yield);
  return txn_result_t(res,10);
}

txn_result_t TpccWorker::txn_new_order_naive1(yield_func_t &yield) {

  // do batch reqs
  tx_->begin(db_logger_);

  // generate the req parameters ////////////////////////////////////////
  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint districtID = RandomNumber(random_generator[cor_id_], 1, 10);
  //const uint districtID = 1;
  const uint customerID = GetCustomerId(random_generator[cor_id_]);
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

  bool allLocal = true;
  std::set<uint64_t> stock_set; //remove identity stock ids;

  // local buffer used store stocks
  RemoteSet::RemoteReqObj remote_stocks[MAX_ITEM];
  int remote_item_ids[MAX_ITEM],local_stocks[MAX_ITEM],local_item_ids[MAX_ITEM];
  uint local_supplies[MAX_ITEM],remote_supplies[MAX_ITEM];

  int num_remote_stocks(0),num_local_stocks(0);

#define MAX_ITEM 15
  const uint numItems = RandomNumber(random_generator[cor_id_], 5, MAX_ITEM);

  for (uint i = 0; i < numItems; i++) {
    bool conflict = false;
    int item_id = GetItemId(random_generator[cor_id_]);
    if (NumWarehouses() == 1 ||
        RandomNumber(random_generator[cor_id_], 1, 100) > g_new_order_remote_item_pct) {
      // locla stock case
      uint supplier_warehouse_id = warehouse_id;
      uint64_t s_key = makeStockKey(supplier_warehouse_id , item_id);
      if(stock_set.find(s_key)!=stock_set.end()) {
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      // remote stock case
      uint64_t s_key;
      uint supplier_warehouse_id;
      do {
        supplier_warehouse_id = RandomNumber(random_generator[cor_id_], 1, NumWarehouses());
        s_key = makeStockKey(supplier_warehouse_id, item_id);
        if(stock_set.find(s_key)!=stock_set.end()){
          conflict = true;
        } else {
          stock_set.insert(s_key);
        }
      } while (supplier_warehouse_id == warehouse_id);
      allLocal =false;

      if(conflict){
        i--;
        continue;
      }

      int pid;
      /* if possible, add remote stock to remote stocks */
      if( (pid = WarehouseToPartition(supplier_warehouse_id)) != current_partition) {

        remote_stocks[num_remote_stocks].tableid = STOC;
        remote_stocks[num_remote_stocks].key     = s_key;
        remote_stocks[num_remote_stocks].pid     = pid;
        remote_stocks[num_remote_stocks].size    = sizeof(stock::value);

        remote_supplies[num_remote_stocks] = supplier_warehouse_id;

        remote_item_ids[num_remote_stocks++] = item_id;

      } else {
        local_item_ids[num_local_stocks] = item_id;
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }

  // Execution phase ////////////////////////////////////////////////////

#if NAIVE == 3
  // do remote reads earlier
#ifndef FARM
  tx_->remoteset->add_batch_imm(REQ_READ,remote_stocks,num_remote_stocks);
#else
  for(uint i = 0; i < num_remote_stocks;++i) {
    tx_->add_to_remote_set(STOC,remote_stocks[i].key,remote_stocks[i].pid);
  }
#endif
#endif

  uint64_t *c_value,*w_value;
  tx_->get(CUST,c_key,(char **)(&c_value),sizeof(customer::value));
  tx_->get(WARE,warehouse_id,(char **)(&w_value),sizeof(warehouse::value));

  uint64_t d_key = makeDistrictKey(warehouse_id,districtID);

  district::value *d_value;
  auto d_seq = tx_->get(DIST,d_key,(char **)(&d_value),sizeof(district::value));

  const auto my_next_o_id = d_value->d_next_o_id;

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;

  tx_->insert(NEWO,no_key, (char *)(&v_no),sizeof(new_order::value));
  d_value->d_next_o_id ++;
  tx_->write(DIST,d_key,(char *)d_value,sizeof(district::value));

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  tx_->insert(ORDE,o_key,(char *)(&v_oo),sizeof(oorder::value));

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;

  tx_->insert(ORDER_INDEX,o_sec,(char *)array_dummy,sizeof(uint64_t) + sizeof(uint64_t));

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {

    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));
    uint64_t s_key = local_stocks[ol_number  - 1];

    stock::value *s_value;
    tx_->get(STOC,s_key,(char **)(&s_value),sizeof(stock::value));

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;
    tx_->write(STOC,s_key,(char *)s_value,sizeof(stock::value));

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }
#if NAIVE == 2

#ifndef FARM
  // do remote reads here
  tx_->remoteset->add_batch_imm(REQ_READ,remote_stocks,num_remote_stocks);
  indirect_yield(yield);
  tx_->remoteset->get_result_imm_batch(0,remote_stocks,num_remote_stocks);
#else
  for(uint i = 0; i < num_remote_stocks;++i) {
    tx_->add_to_remote_set(STOC,remote_stocks[i].key,remote_stocks[i].pid);
  }
  indirect_yield(yield);
#endif

#elif NAIVE == 3
  indirect_yield(yield);
#ifndef FARM
  tx_->remoteset->get_result_imm_batch(0,remote_stocks,num_remote_stocks);
#else
#endif  // one-sided version do nothing
#endif

  // operation remote objects
  for(uint i = 0;i < num_remote_stocks;++i) {

    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    item::value *i_value;

    tx_->get(ITEM, ol_i_id, (char **)(&(i_value)),sizeof(item::value));

    stock::value *s_value;

    // parse the result
    tx_->get_cached(i,(char **)(&s_value));
    //fprintf(stdout,"previous quantity %d,\n",s_value->s_quantity);
#if 1
    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;
    //fprintf(stdout,"after quantity %d,\n",s_value->s_quantity);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
#endif
    // prepare lock and write records
    tx_->remote_write(i,(char *)s_value,sizeof(stock::value));

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    tx_->insert(ORLI,ol_key,(char *)(&v_ol),sizeof(order_line::value));
  }

  // Commit phase ////////////////////////////////////////////////////
#if !TX_ONLY_EXE
  bool res = tx_->end(yield);
#else
  bool res = true;
#ifndef FARM
  tx_->remoteset->update_read_buf();
  tx_->remoteset->update_write_buf();
#endif
#endif
#if 0
  // sanity checks
  if(res == true && current_partition == 0) {
    tx_->begin(db_logger_);
    tx_->remoteset->add_batch_imm(REQ_READ,remote_stocks,num_remote_stocks);
    indirect_yield(yield);
    tx_->remoteset->get_result_imm_batch(0,remote_stocks,num_remote_stocks);
    fprintf(stdout,"checks...\n");
    for(uint i = 0;i < num_remote_stocks;++i) {
      stock::value *s_value;
      tx_->get_cached(i,(char **)(&s_value));
      fprintf(stdout,"after quantity %d,\n",s_value->s_quantity);
    }
    assert(false);
  }
#endif
  return txn_result_t(res,10);
}

txn_result_t TpccWorker::txn_payment_naive1(yield_func_t &yield) {
  assert(false); // not implemented yet
  return txn_result_t(true,10);
}




/* End namespace tpcc */
}
/* End namespace nocc framework */
}
}
