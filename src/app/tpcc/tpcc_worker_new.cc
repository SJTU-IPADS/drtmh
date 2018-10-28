// new implementation of TPC-C

#include "tpcc_worker.h"

namespace nocc {
namespace oltp {

extern __thread util::fast_random   *random_generator;

static std::map<int,int> *warehouse_hotmap = NULL;

namespace tpcc {

extern int g_new_order_remote_item_pct;

txn_result_t TpccWorker::txn_new_order_new(yield_func_t &yield) {

#if CHECKS
  if(worker_id_ != 0 || current_partition != 0)
    return txn_result_t(true,1);
#endif

  rtx_->begin(yield);

  // generate the req parameters ////////////////////////////////////////
  const uint warehouse_id = PickWarehouseId(random_generator[cor_id_], warehouse_id_start_, warehouse_id_end_);
  const uint districtID = RandomNumber(random_generator[cor_id_], 1, 10);
  //const uint districtID = 1;
  const uint customerID = GetCustomerId(random_generator[cor_id_]);
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);

#define MAX_ITEM 15

  bool allLocal = true;
  std::set<uint64_t> stock_set; //remove identity stock ids;

  // local buffer used store stocks
  uint64_t remote_stocks[MAX_ITEM];
  int remote_item_ids[MAX_ITEM],local_stocks[MAX_ITEM],local_item_ids[MAX_ITEM];
  uint local_supplies[MAX_ITEM],remote_supplies[MAX_ITEM];

  int num_remote_stocks(0),num_local_stocks(0);

  const uint numItems = RandomNumber(random_generator[cor_id_], 5, MAX_ITEM);
#if OR
  START(read);
#endif
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

#if OR  // issue the reads here
        auto idx = rtx_->pending_read<STOC,stock::value>(WarehouseToPartition(supplier_warehouse_id),s_key,yield);
        rtx_->add_to_write();
#endif
      } else {
        local_item_ids[num_local_stocks] = item_id;
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_stocks[num_local_stocks++] = s_key;
      }
    }
  }

  // Execution phase ////////////////////////////////////////////////////
  rtx_->read<CUST,customer::value>(current_partition,c_key,yield);
  rtx_->read<WARE,warehouse::value>(current_partition,warehouse_id,yield);

  uint64_t d_key = makeDistrictKey(warehouse_id,districtID);

  auto idx = rtx_->read<DIST,district::value>(current_partition,d_key,yield);
  district::value *d_value = rtx_->get_readset<district::value>(idx,yield);

  const auto my_next_o_id = d_value->d_next_o_id;

  d_value->d_next_o_id ++;
  rtx_->add_to_write(); // add the last item to readset

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  new_order::value v_no;
  rtx_->insert<NEWO,new_order::value>(current_partition,no_key,&v_no,yield);

  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);

  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0;
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  uint64_t o_sec = makeOrderIndex(warehouse_id,districtID,
                                  customerID,my_next_o_id);
  rtx_->insert<ORDE,oorder::value>(current_partition,o_key,(&v_oo),yield); // TODO!!

  uint64_t *idx_value;

  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;
  typedef struct { uint64_t a;uint64_t b; } order_index_type_t;

  rtx_->insert<ORDER_INDEX,order_index_type_t>(current_partition,o_sec,(order_index_type_t *)array_dummy,yield);

  for (uint ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    //      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = local_item_ids[ol_number - 1];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    auto idx = rtx_->read<ITEM,item::value>(current_partition,ol_i_id,yield);
    item::value *i_value = rtx_->get_readset<item::value>(idx,yield);

    uint64_t s_key = local_stocks[ol_number  - 1];

    idx = rtx_->read<STOC,stock::value>(current_partition,s_key,yield);
    stock::value *s_value = rtx_->get_readset<stock::value>(idx,yield);

    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += (-int32_t(ol_quantity) + 91);

    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;
    rtx_->add_to_write();

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    rtx_->insert<ORLI,order_line::value>(current_partition,ol_key,(&v_ol),yield);
  }

#if OR // fetch remote records
  indirect_yield(yield);
  END(read);
#endif

#if !OR
  START(read);
#endif
  /* operation remote objects */
  for(uint i = 0;i < num_remote_stocks;++i) {

    const uint ol_i_id = remote_item_ids[i];
    const uint ol_quantity = RandomNumber(random_generator[cor_id_], 1, 10);

    auto idx = rtx_->read<ITEM,item::value>(current_partition,ol_i_id,yield);
    item::value *i_value = rtx_->get_readset<item::value>(idx,yield);

    uint64_t s_key = remote_stocks[i];

    // fetch remote objects
    // parse the result
#if OR
    idx = i;
    stock::value *s_value = rtx_->get_writeset<stock::value>(idx,yield);
#else
    idx = rtx_->read<STOC,stock::value>(WarehouseToPartition(stockKeyToWare(s_key)),s_key,yield);
    stock::value *s_value = rtx_->get_readset<stock::value>(idx,yield);
    rtx_->add_to_write();
#endif
    assert(s_value != NULL);
#if 1
#if CHECKS
    LOG(2) << "skey " << s_key << ",get remote stock, quantity " << s_value->s_quantity;
#endif
    if (s_value->s_quantity - ol_quantity >= 10)
      s_value->s_quantity -= ol_quantity;
    else
      s_value->s_quantity += -int32_t(ol_quantity) + 91;
#if CHECKS
    LOG(2) << "modify quantity to " << s_value->s_quantity;
#endif
    s_value->s_ytd += ol_quantity;
    s_value->s_remote_cnt += 1;
#endif

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, num_local_stocks + i + 1);
    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_value->i_price;
    v_ol.ol_supply_w_id = int32_t(remote_supplies[i]);
    v_ol.ol_quantity = int8_t(ol_quantity);
    rtx_->insert<ORLI,order_line::value>(current_partition,ol_key,(&v_ol),yield);
  }
#if !OR
  END(read);
#endif
  bool res = rtx_->commit(yield);
#if CHECKS
  // some checks
  rtx_->begin(yield);
  for(uint i = 0;i < num_remote_stocks;++i) {
    uint64_t s_key = remote_stocks[i];
    stock::value *s = rtx_->get<STOC,stock::value>(WarehouseToPartition(stockKeyToWare(s_key)),s_key,yield);
    LOG(2) << "recheck skey " << s_key << ",get remote stock, quantity " << s->s_quantity;
  }
  sleep(1);
#endif
  return txn_result_t(res,1);
}

TpccWorker::~TpccWorker() {

}

} // namespace tpcc

} // namespace oltp

} // namespace nocc
