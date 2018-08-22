#include "tpce_worker.h"

#include "rtx/occ_iterator.hpp"

using namespace TPCE;

namespace nocc {

namespace oltp {

extern __thread util::fast_random   *random_generator;

namespace tpce {

/* fixed tables */
extern std::map<std::string,trade_type::value *>    TpceTradeHash;
extern std::map<std::string,zip_code::value *>      TpceZipCode;
extern std::map<std::string,industry::value *>      TpceIndustry;
extern std::map<uint64_t,   news_item::value*>      TpceNewsItem;
extern std::map<uint64_t,   address::value *>       TpceAddress;
extern std::map<std::string,int>                    TpceExchangeMap;
extern std::map<std::string,status_type::value *>   TpceStatusType;
extern std::map<std::string,std::string>            TpceSector;
extern std::map<std::string,std::string>            SecurityToSector;
extern std::map<uint64_t,uint64_t>                  WatchList;
extern std::map<uint64_t,std::vector<std::string> > WatchItem;
extern std::map<std::string,std::uint64_t >         SecurityToId;
extern std::map<std::string,uint64_t> SecurityToCompany;

txn_result_t TpceWorker::txn_cp_new(yield_func_t &yield) {

  TCustomerPositionTxnInput input;
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  input_generator_->GenerateCustomerPositionInput(input);

  rtx_->begin(yield);

  uint64_t cust_id = 0;
  if(input.cust_id == 0) {
    /* fetching */
    uint64_t sec_key = makeTaxCustKey(input.tax_id,0);

    rtx::RTXIterator iter(rtx_,SEC_TAX_CUST,true);
    iter.seek(sec_key);
    if(iter.valid()) {
      uint64_t *k = (uint64_t *)iter.key();
      cust_id = k[2];
      //LOG(3) << "fetch " << cust_id << " from taxid " << input.tax_id;
    } else {
      assert(false);
    }
  } else
    cust_id = input.cust_id;

  std::vector<uint64_t> acct_ids;

  customers::value *vc = rtx_->get<ECUST,customers::value>(current_partition,
                                                           cust_id,yield);
  assert(vc != NULL);

  uint64_t c_key_start = makeCustAcctKey(cust_id,0);

  rtx::RTXIterator c_iter(rtx_,CUST_ACCT,false);
  c_iter.seek(c_key_start);

  const char *min_s ="\0";
  while(c_iter.valid()) {
    uint64_t *key = (uint64_t *)c_iter.key();
    if(key[0] != cust_id) break;

    uint64_t acct_id = key[1];
    acct_ids.push_back(acct_id);

    customer_account::value *cv = rtx_->get<CUSTACCT,customer_account::value>(current_partition,
                                                                              acct_id,yield);
    rtx::RTXIterator hs_iter(rtx_,HOLDING_SUM,false);

    uint64_t hs_start_key = makeHSKey(acct_id,(const char *)(min_s));
    hs_iter.seek(hs_start_key);
    while(hs_iter.valid()) {
      uint64_t *hs_key = (uint64_t *)hs_iter.key();
      if(hs_key[0] != acct_id)
        break;

      assert(SecurityToId.find((char *)(&hs_key[1])) != SecurityToId.end());

      uint64_t co_id = SecurityToCompany[(char *)(&hs_key[1])];
      uint64_t lt_id = SecurityToId[(char *)(&hs_key[1])];

      int pid = companyToPartition(co_id);

      //      LOG(4) << "fetch lt from " << pid;
      last_trade::value *lt = rtx_->get<LT1,last_trade::value>(pid,
                                                               lt_id,yield);
      //LOG(4) << "fetch lt from " << pid << "; check value " << lt->lt_price;
      hs_iter.next();
    }

    c_iter.next();
  }

  int hist_len = 0;

  if(input.get_history) {
    ASSERT(input.acct_id_idx < acct_ids.size());
    uint64_t acct_id = acct_ids[input.acct_id_idx];

    uint64_t min_ca_tid_sec = makeSecondCATrade(acct_id,std::numeric_limits<uint64_t>::min(),
                                                std::numeric_limits<uint64_t>::min());
    rtx::RTXIterator iter(rtx_,SEC_CA_TRADE,true);
    iter.seek(min_ca_tid_sec);

    int count = 0;
    while(iter.valid()) {
      uint64_t *key = (uint64_t *)(iter.key());
      if(count++ == 10 || acct_id != key[0])
        break;
      trade::value *tv = rtx_->get<TRADE,trade::value>(current_partition,key[2],yield);

      rtx::RTXIterator th_iter(rtx_,TRADE_HIST,false);
      uint64_t min_th_key = makeTHKey(key[2],std::numeric_limits<uint64_t>::min(),min_s);
      th_iter.seek(min_th_key);

      while(th_iter.valid()) {
        uint64_t *th_key = (uint64_t *)th_iter.key();
        trade_history::value *v = rtx_->get<TRADE_HIST,trade_history::value>(current_partition,
                                                                             (uint64_t)th_key,yield);
        if(th_key[0] != key[2])
          break;
        hist_len += 1;
        th_iter.next();
      }
      iter.next();
    }
  }

  bool res = rtx_->commit(yield);
  return txn_result_t(res,1);
}

} // namespace oltp

} // namespace tpce

}
