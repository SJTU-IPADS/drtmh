#include "tx_config.h"

#include "tpcc_worker.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/dbrad.h"

#include <set>

using namespace std;

extern int verbose;

namespace nocc {

using namespace db;

namespace oltp {

namespace tpcc {

void convertString(char *newstring, const char *oldstring, int size) {
  for (int i = 0; i < 8; i++)
    if (i < size)
      newstring[7 -i] = oldstring[i];
    else newstring[7 -i] = '\0';

  for (int i=8; i<16; i++)
    if (i < size)
      newstring[23 -i] = oldstring[i];
    else newstring[23 -i] = '\0';
}

bool compareCustomerIndex(uint64_t key, uint64_t bound){
  uint64_t *k = (uint64_t *)key;
  uint64_t *b = (uint64_t *)bound;
  for (int i=0; i < 5; i++) {
    if (k[i] > b[i]) return false;
    if (k[i] < b[i]) return true;
  }
  return true;
}

uint64_t makeCustomerIndex(int32_t w_id, int32_t d_id, string s_last, string s_first) {
  uint64_t *seckey = new uint64_t[5];
  int32_t did = d_id + (w_id * 10);
  seckey[0] = did;
  convertString((char *)(&seckey[1]), s_last.data(), s_last.size());
  convertString((char *)(&seckey[3]), s_first.data(), s_last.size());
  return (uint64_t)seckey;
}

void TpccWarehouseLoader::load() {

  LOG(2) << "loading warehouses.";
  RThreadLocalInit();

  string obj_buf;

  uint64_t warehouse_total_sz = 0, n_warehouses = 0;
  try {
    //  fprintf(stdout,"meta length %d %d\n",META_LENGTH,SI_META_LEN);
    vector<warehouse::value> warehouses;
    for (uint i = GetStartWarehouse(partition_); i <= GetEndWarehouse(partition_); i++) {

      const warehouse::key k(i);
      const string w_name = RandomStr(random_generator_, RandomNumber(random_generator_, 6, 10));
      const string w_street_1 = RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20));
      const string w_street_2 = RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20));
      const string w_city = RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20));
      const string w_state = RandomStr(random_generator_, 3);
      const string w_zip = "123456789";

      int w_size = store_->_schemas[WARE].total_len;

#if ONE_SIDED_READ == 1
      char *wrapper = (char *)Rmalloc(w_size);
#else
      char *wrapper = (char *)malloc(w_size);
#endif

      memset(wrapper, 0, META_LENGTH + sizeof(warehouse::value));
      warehouse::value *v = (warehouse::value *)(wrapper + META_LENGTH);
      v->w_ytd = 300000 * 100;
      v->w_tax = (float) RandomNumber(random_generator_, 0, 2000) / 10000.0;
      v->w_name.assign(w_name);
      v->w_street_1.assign(w_street_1);
      v->w_street_2.assign(w_street_2);
      v->w_city.assign(w_city);
      v->w_state.assign(w_state);
      v->w_zip.assign(w_zip);

      checker::SanityCheckWarehouse(&k, v);
      const size_t sz = Size(*v);
      warehouse_total_sz += sz;
      n_warehouses++;

      store_->Put(WARE, i, (uint64_t *)wrapper);

      warehouses.push_back(*v);

      // sanity check
      assert(store_->Get(WARE,i) != NULL);
    }
  } catch (...) {
    // shouldn't abort on loading!
    ALWAYS_ASSERT(false);
  }
  LOG(2) << "[TPCC] finished loading warehouse.";
  LOG(2) << "[TPCC]   * average warehouse record length: "
         << (double(warehouse_total_sz)/double(n_warehouses)) << " bytes.";
  /* end loading warehouse */
}

void TpccDistrictLoader::load() {

  RThreadLocalInit();
  string obj_buf;

  const ssize_t bsize = -1;
  uint64_t district_total_sz = 0, n_districts = 0;
  try {
    uint cnt = 0;
    for (uint w = GetStartWarehouse(partition_); w <= GetEndWarehouse(partition_); w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
        uint64_t key = makeDistrictKey(w, d);
        const district::key k(makeDistrictKey(w, d));

        int d_size = store_->_schemas[DIST].total_len;

#if ONE_SIDED_READ == 1
        char *wrapper = (char *)Rmalloc(d_size);
#else
        char *wrapper = (char *)malloc(d_size);
#endif

        memset(wrapper, 0, META_LENGTH + sizeof(district::value) + sizeof(uint64_t));
        district::value *v = (district::value *)(wrapper + META_LENGTH);
        v->d_ytd = 30000 * 100; //notice i did the scale up
        v->d_tax = (float) (RandomNumber(random_generator_, 0, 2000) / 10000.0);
        v->d_next_o_id = 3001;
        v->d_name.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 6, 10)));
        v->d_street_1.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20)));
        v->d_street_2.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20)));
        v->d_city.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20)));
        v->d_state.assign(RandomStr(random_generator_, 3));
        v->d_zip.assign("123456789");

        const size_t sz = Size(*v);
        district_total_sz += sz;
        n_districts++;

        store_->Put(DIST, key, (uint64_t *)wrapper);
        assert(store_->Get(DIST,key) != NULL);
      }
    }
  } catch (...) {
    // shouldn't abort on loading!
    ALWAYS_ASSERT(false);
  }
  LOG(2) << "[TPCC] finished loading district.";
  LOG(2) << "[TPCC]   * average district record length: "
         << (double(district_total_sz)/double(n_districts)) << " bytes.";
}

void TpccCustomerLoader::load() {

  string obj_buf;

  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  const size_t batchsize =
      NumCustomersPerDistrict() ;

  const size_t nbatches =
      (batchsize > NumCustomersPerDistrict()) ?
      1 : (NumCustomersPerDistrict() / batchsize);

  uint64_t total_sz = 0;

  for (uint w = w_start; w <= w_end; w++) {
    //      if (pin_cpus)
    //        PinToWarehouseId(w);
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      for (uint batch = 0; batch < nbatches;) {
        //          scoped_str_arena s_arena(arena);
        const size_t cstart = batch * batchsize;
        const size_t cend = std::min((batch + 1) * batchsize, NumCustomersPerDistrict());
        try {
          for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
            const uint c = cidx0 + 1;
            uint64_t key = makeCustomerKey(w, d, c);
            const customer::key k(key);

            int c_size = store_->_schemas[CUST].total_len;
            char *wrapper = (char *)malloc(c_size);
            memset(wrapper, 0, META_LENGTH + sizeof(customer::value) + sizeof(uint64_t));
            customer::value *v = (customer::value *)(wrapper + META_LENGTH);
            v->c_discount = (float) (RandomNumber(random_generator_, 1, 5000) / 10000.0);
            if (RandomNumber(random_generator_, 1, 100) <= 10)
              v->c_credit.assign("BC");
            else
              v->c_credit.assign("GC");

            if (c <= 1000)
              v->c_last.assign(GetCustomerLastName(random_generator_, c - 1));
            else
              v->c_last.assign(GetNonUniformCustomerLastNameLoad(random_generator_));

            v->c_first.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 8, 16)));
            v->c_credit_lim = 50000;

            v->c_balance = -10;
            v->c_balance_1 = 0;
            v->c_ytd_payment = 10;
            v->c_payment_cnt = 1;
            v->c_delivery_cnt = 0;

            v->c_street_1.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20)));
            v->c_street_2.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20)));
            v->c_city.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 10, 20)));
            v->c_state.assign(RandomStr(random_generator_, 3));
            v->c_zip.assign(RandomNStr(random_generator_, 4) + "11111");
            v->c_phone.assign(RandomNStr(random_generator_, 16));
            v->c_since = GetCurrentTimeMillis();
            v->c_middle.assign("OE");
            v->c_data.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 300, 500)));

            const size_t sz = Size(*v);
            total_sz += sz;

            store_->Put(CUST, key, (uint64_t *)wrapper);
            // customer name index

            uint64_t sec = makeCustomerIndex(w, d,
                                             v->c_last.str(true), v->c_first.str(true));

            //    uint64_t* mn = store_->_indexs[CUST_INDEX].Get(sec);
            uint64_t *mn = store_->Get(CUST_INDEX,sec);
            if (mn == NULL) {
              char *ciwrap = new char[META_LENGTH + sizeof(uint64_t)*2  + sizeof(uint64_t)];
              memset(ciwrap, 0, META_LENGTH);
              uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);

              prikeys[0] = 1; prikeys[1] = key;
              //printf("key %ld\n",key);
              store_->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
            }
            else {
              assert(false);
              printf("ccccc\n");
              uint64_t *value = (uint64_t *)((char *)mn + META_LENGTH);
              int num = value[0];
              char *ciwrap = new char[META_LENGTH + sizeof(uint64_t)*(num+2)];
              memset(ciwrap, 0, META_LENGTH);
              uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);
              prikeys[0] = num + 1;
              for (int i=1; i<=num; i++)
                prikeys[i] = value[i];
              prikeys[num+1] = key;
              store_->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
            }
            char *hwrap = new char[META_LENGTH + sizeof(history::value)];
            memset(hwrap, 0, META_LENGTH);

            uint64_t hkey = makeHistoryKey(c,d,w,d,w);

            history::key k_hist(makeHistoryKey(c,d,w,d,w));

            history::value *v_hist = (history::value*)(hwrap + META_LENGTH);
            v_hist->h_amount = 10;
            v_hist->h_data.assign(RandomStr(random_generator_, RandomNumber(random_generator_, 10, 24)));

            store_->Put(HIST, hkey, (uint64_t *)hwrap);
          }
          batch++;

        } catch (...) {
          assert(false);
        }
      }
    }
    /* end iterating warehosue */
  }

  if (verbose) {
    LOG(2) << "[TPCC] finished loading customer.";
    LOG(2) << "[TPCC]   * average customer record length: "
           << (double(total_sz) / double(NumWarehouses()
                                         * NumDistrictsPerWarehouse() * NumCustomersPerDistrict()))
           << " bytes.";
  }
}

void TpccOrderLoader::load() {

  string obj_buf;
  uint64_t order_line_total_sz = 0, n_order_lines = 0;
  uint64_t oorder_total_sz = 0, n_oorders = 0;
  uint64_t new_order_total_sz = 0, n_new_orders = 0;

  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  for (uint w = w_start; w <= w_end; w++) {
    //      if (pin_cpus)
    //        PinToWarehouseId(w);
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      set<uint> c_ids_s;
      vector<uint> c_ids;
      while (c_ids.size() != NumCustomersPerDistrict()) {
        const auto x = (random_generator_.next() % NumCustomersPerDistrict()) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }
      for (uint c = 1; c <= NumCustomersPerDistrict();) {
        try {
          uint64_t okey = makeOrderKey(w, d, c);
          const oorder::key k_oo(okey);

          char *wrapper = new char[META_LENGTH+sizeof(oorder::value) + sizeof(uint64_t)];
          memset(wrapper, 0 ,META_LENGTH + sizeof(oorder::value) + sizeof(uint64_t));
          oorder::value *v_oo = (oorder::value *)(wrapper + META_LENGTH);
          v_oo->o_c_id = c_ids[c - 1];
          if (c < 2101)
            v_oo->o_carrier_id = RandomNumber(random_generator_, 1, 10);
          else
            v_oo->o_carrier_id = 0;
          v_oo->o_ol_cnt = RandomNumber(random_generator_, 5, 15);

          v_oo->o_all_local = 1;
          v_oo->o_entry_d = GetCurrentTimeMillis();

          const size_t sz = Size(*v_oo);
          oorder_total_sz += sz;
          n_oorders++;
          store_->Put(ORDE, okey, (uint64_t *)wrapper);

          uint64_t sec = makeOrderIndex(w, d, v_oo->o_c_id, c);

          char *oiwrapper = new char[META_LENGTH+16+sizeof(uint64_t)];
          memset(oiwrapper, 0 ,META_LENGTH + 16 + sizeof(uint64_t));
          uint64_t *prikeys = (uint64_t *)(oiwrapper+META_LENGTH);
          prikeys[0] = 1; prikeys[1] = okey;
          store_->Put(ORDER_INDEX, sec, (uint64_t *)oiwrapper);

          const oorder_c_id_idx::key k_oo_idx(makeOrderIndex(w, d, v_oo->o_c_id, c));
          const oorder_c_id_idx::value v_oo_idx(0);

          if (c >= 2101) {
            uint64_t nokey = makeNewOrderKey(w, d, c);
            const new_order::key k_no(makeNewOrderKey(w, d, c));

            char* nowrap = new char[META_LENGTH + sizeof(new_order::value) + sizeof(uint64_t)];
            memset(nowrap, 0, META_LENGTH + sizeof(new_order::value) + sizeof(uint64_t));
            new_order::value *v_no = (new_order::value *)(nowrap+META_LENGTH );

            checker::SanityCheckNewOrder(&k_no, v_no);
            const size_t sz = Size(*v_no);
            new_order_total_sz += sz;
            n_new_orders++;
            store_->Put(NEWO, nokey, (uint64_t *)nowrap);
          }

#if 1
          for (uint l = 1; l <= uint(v_oo->o_ol_cnt); l++) {
            uint64_t olkey = makeOrderLineKey(w, d, c, l);
            const order_line::key k_ol(makeOrderLineKey(w, d, c, l));

            char *olwrapper = new char[META_LENGTH+sizeof(order_line::value) + sizeof(uint64_t)];
            memset(olwrapper, 0 ,META_LENGTH + sizeof(order_line::value) + sizeof(uint64_t));
            order_line::value *v_ol = (order_line::value *)(olwrapper + META_LENGTH);
            v_ol->ol_i_id = RandomNumber(random_generator_, 1, 100000);
            if (c < 2101) {
              v_ol->ol_delivery_d = v_oo->o_entry_d;
              v_ol->ol_amount = 0;
            } else {
              v_ol->ol_delivery_d = 0;
              /* random within [0.01 .. 9,999.99] */
              v_ol->ol_amount = (float) (RandomNumber(random_generator_, 1, 999999) / 100.0);
            }

            v_ol->ol_supply_w_id = w;
            v_ol->ol_quantity = 5;
            // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
            checker::SanityCheckOrderLine(&k_ol, v_ol);
            const size_t sz = Size(*v_ol);
            order_line_total_sz += sz;
            n_order_lines++;
            store_->Put(ORLI, olkey, (uint64_t *)olwrapper);
          }
#endif
          c++;
        } catch (...) {
        }
      }
    }

  }
  /* order loader */
}

void TpccStockLoader::load() {

  RThreadLocalInit();

  string obj_buf, obj_buf1;
  uint64_t stock_total_sz = 0, n_stocks = 0;
  const uint w_start = GetStartWarehouse(partition_);
  const uint w_end   = GetEndWarehouse(partition_);

  for (uint w = w_start; w <= w_end; w++) {
    const size_t batchsize =  NumItems() ;
    const size_t nbatches = (batchsize > NumItems()) ? 1 : (NumItems() / batchsize);

    for (uint b = 0; b < nbatches;) {

      try {
        const size_t iend = std::min((b + 1) * batchsize + 1, NumItems());
        for (uint i = (b * batchsize + 1); i <= iend; i++) {
          uint64_t key = makeStockKey(w, i);

          int s_size = store_->_schemas[STOC].total_len;
          s_size = Round<int>(s_size,CACHE_LINE_SIZE);

          char *wrapper = NULL;

          // If main store, allocation memory on RDMA heap
          if(ONE_SIDED_READ && !backup_)
            wrapper = (char *)Rmalloc(s_size);
          else {
            wrapper = (char *)malloc(s_size);
          }

          memset(wrapper, 0, META_LENGTH + sizeof(stock::value));
          stock::value *v = (stock::value *)(wrapper + META_LENGTH);
          v->s_quantity = RandomNumber(random_generator_, 10, 100);
          v->s_ytd = 0;
          v->s_order_cnt = 0;
          v->s_remote_cnt = 0;

          const int len = RandomNumber(random_generator_, 26, 50);
          if (RandomNumber(random_generator_, 1, 100) > 10) {
            const string s_data = RandomStr(random_generator_, len);
          } else {
            const int startOriginal = RandomNumber(random_generator_, 2, (len - 8));
            const string s_data = RandomStr(random_generator_, startOriginal + 1)
                                  + "ORIGINAL" + RandomStr(random_generator_, len - startOriginal - 7);
          }
          const stock::key k(makeStockKey(w, i));
          checker::SanityCheckStock(&k, v);
          const size_t sz = Size(*v);
          stock_total_sz += sz;
          n_stocks++;
          auto node = store_->Put(STOC, key, (uint64_t *)wrapper,sizeof(stock::value));
          node->off = (uint64_t)wrapper - (uint64_t)(cm->conn_buf_);
          assert(node->off != 0);
        }
        b++;
      } catch (...) {
        if (verbose)
          cerr << "[WARNING] stock loader loading abort" << endl;
      }
    }
  }

  if (verbose) {

    cerr << "[TPCC] finished loading stock" << endl;
    cerr << "[TPCC]   * average stock record length: "
         << (double(stock_total_sz)/double(n_stocks)) << " bytes" << endl;
  }
}

void TpccItemLoader::load() {
  string obj_buf;
  uint64_t total_sz = 0;
  try {
    for (uint i = 1; i <= NumItems(); i++) {
      const item::key k(i);

      int i_size = store_->_schemas[ITEM].total_len;
      char *wrapper = (char *)malloc(i_size);
      memset(wrapper, 0, META_LENGTH);
      item::value *v = (item::value *)(wrapper + META_LENGTH);;
      const string i_name = RandomStr(random_generator_, RandomNumber(random_generator_, 14, 24));
      v->i_name.assign(i_name);
      v->i_price = (float) RandomNumber(random_generator_, 100, 10000) / 100.0;
      const int len = RandomNumber(random_generator_, 26, 50);
      if (RandomNumber(random_generator_, 1, 100) > 10) {
        const string i_data = RandomStr(random_generator_, len);
        v->i_data.assign(i_data);
      } else {
        const int startOriginal = RandomNumber(random_generator_, 2, (len - 8));
        const string i_data = RandomStr(random_generator_, startOriginal + 1) +
                              "ORIGINAL" + RandomStr(random_generator_, len - startOriginal - 7);
        v->i_data.assign(i_data);
      }
      v->i_im_id = RandomNumber(random_generator_, 1, 10000);
      checker::SanityCheckItem(&k, v);
      const size_t sz = Size(*v);
      total_sz += sz;

      store_->Put(ITEM, i, (uint64_t *)wrapper);
    }
  } catch (...) {
    ALWAYS_ASSERT(false);
  }
  if (verbose) {
    cerr << "[TPCC] finished loading item" << endl;
    cerr << "[TPCC]   * average item record length: "
         << (double(total_sz)/double(NumItems())) << " bytes" << endl;
  }

}
/* end namespace */
};
}
}
