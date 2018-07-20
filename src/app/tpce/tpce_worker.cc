#include "tx_config.h"

#include "tpce_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"

#include "db/forkset.h"

#include "util/mapped_log.h"

#include "egen/EGenLoader_stdafx.h"
#include "egen/EGenGenerateAndLoad.h"

//#include "framework/rpc.h"
#include "framework/req_buf_allocator.h"

#include <boost/bind.hpp>
#include <queue>

/* control flags */
//#define RECORD
#define TX_DUMMY_WRITE

#ifdef OCC_TX
#undef TX_DUMMY_WRITE
extern __thread RemoteHelper *remote_helper;
#endif

extern size_t total_partition;
extern size_t current_partition;

extern __thread MappedLog local_log;

using namespace TPCE;

namespace nocc {

// db utilities
extern __thread TXHandler   **txs_;
extern __thread oltp::RPCMemAllocator *msg_buf_alloctors;

static SpinLock exit_lock;

namespace oltp {

extern __thread util::fast_random   *random_generator;
extern RdmaCtrl *cm;

// extern __thread uint *next_coro_id_arr_;
namespace tpce {

/* maybe i should replace the implementation of these queues */
__thread std::queue<TTradeResultTxnInput> *tradeResultQueue;
__thread std::queue<TTickerEntry>         *marketFeedQueue;

__thread std::map<uint64_t,bool> *processed_trades = NULL;
__thread int8_t *market_feed_process_idx = NULL;
__thread TTickerEntry (*tickerBuffer)[10]  = NULL;

__thread TTradeResultTxnInput **tradeResultCache = NULL;

uint64_t type_margin_executed;
uint64_t num_remote_securities;

uint64_t prev_executed;
uint64_t prev_numbered;

extern unsigned g_txn_workload_mix[10];
extern uint64_t lastTradeId;
extern CInputFiles *inputFiles;
extern CEGenLogger *logger;

extern int totalCustomer;
extern int scaleFactor;
extern int mStartCustomer;
extern int tradeDays;

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

/**/
extern std::map<std::string,uint64_t> TpceTradeTypeMap;
extern std::map<int32_t, double> TpceTaxMap;

extern std::map<std::string,uint64_t> SecurityToCompany;
extern std::map<std::string,uint64_t> CONameToId;

extern std::map<std::string,uint64_t> IndustryNametoID;

TpceWorker::TpceWorker(unsigned int worker_id,unsigned long seed,MemDB *store,
                       spin_barrier *a,spin_barrier *b,BenchRunner *context) :
    TpceMixin(store),
    BenchWorker(worker_id,true,seed,0,a,b,context)
{
  /* init input generator */
  PDriverCETxnSettings mDriverCETxnSettings = new TDriverCETxnSettings();

  fprintf(stdout,"[Worker %d] Create input generator.\n",worker_id);
#if 1
  input_generator_ = new CCETxnInputGenerator(*inputFiles,
                                              totalCustomer,totalCustomer,						       /* 24 hours per da*/
                                              scaleFactor,tradeDays * HoursPerWorkDay,
                                              /* current the partition percentage is 100,
                                                 since for equal partitioning the range is not important
                                              */
                                              mStartCustomer,accountPerPartition,100,
                                              logger,mDriverCETxnSettings);
#else
  /* single server case */
  input_generator_ = new CCETxnInputGenerator(*inputFiles,
                                              totalCustomer,totalCustomer,
                                              scaleFactor,tradeDays * 24,logger,mDriverCETxnSettings);
#endif

  /* re-calcualte the MaxPrePopulatedTrade */
  input_generator_->m_iMaxActivePrePopulatedTradeID =
      (INT64)(( (INT64)(HoursPerWorkDay * tradeDays) * SecondsPerHour *
                /* 1.01 to account for rollbacks */
                (accountPerPartition / scaleFactor )) * iAbortTrade / INT64_CONST(100) );
  ;
  trade_id_ = lastTradeId + 1;
  fprintf(stdout,"init trade_id %lu, required %lld\n",trade_id_,
          input_generator_->m_iMaxActivePrePopulatedTradeID);
  assert(lastTradeId < std::numeric_limits<uint32_t>::max());
}

void TpceWorker::register_callbacks() {

  fprintf(stdout,"TPCE register callbacks\n");
  one_shot_callbacks[TX_CP]  = bind(&TpceWorker::customer_position_piece,this,_1,_2,_3,_4);
  one_shot_callbacks[TX_SED] = bind(&TpceWorker::security_detail_piece,this,_1,_2,_3,_4);
  one_shot_callbacks[TX_TL0] = bind(&TpceWorker::trade_lookup_piece0,this,_1,_2,_3,_4);
  one_shot_callbacks[TX_TL1] = bind(&TpceWorker::trade_lookup_piece1,this,_1,_2,_3,_4);
  one_shot_callbacks[TX_BV]  = bind(&TpceWorker::broker_volumn_piece,this,_1,_2,_3,_4);
  one_shot_callbacks[TX_MW]  = bind(&TpceWorker::market_watch_piece,this,_1,_2,_3,_4);

  rpc_->register_callback(bind(&TpceWorker::add_market_feed,this,_1,_2,_3,_4), \
                          RPC_ADD);
}

uint64_t TpceWorker::get_last_trade_id() {
  uint64_t res = encode_trade_id(trade_id_,worker_id_,cor_id_);
  trade_id_ += 1;
  assert(trade_id_ < std::numeric_limits<uint32_t>::max());
  return res;
}

workload_desc_vec_t TpceWorker::get_workload() const {
  return _get_workload();
}

workload_desc_vec_t TpceWorker::_get_workload() {

  workload_desc_vec_t w;
  unsigned m = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
    m += g_txn_workload_mix[i];
  assert(m == 100);
  if (g_txn_workload_mix[0]) {
    w.push_back(workload_desc("CustomerPosition", g_txn_workload_mix[0]/100.0, TxnCustomerPos));
  }
  if (g_txn_workload_mix[1]) {
    w.push_back(workload_desc("TradeOrder",g_txn_workload_mix[1]/100.0, TxnTradeOrder));
  }
  if (g_txn_workload_mix[2]) {
    w.push_back(workload_desc("TradeResult",g_txn_workload_mix[2]/100.0, TxnTradeResult));
  }
  if (g_txn_workload_mix[3]) {
    w.push_back(workload_desc("SecurityDetail",g_txn_workload_mix[3]/100.0,TxnSecurityDetail));
  }
  if(g_txn_workload_mix[4]) {
    w.push_back(workload_desc("TradeLookup",g_txn_workload_mix[4]/100.0,TxnTradeLookup));
  }
  if(g_txn_workload_mix[5]) {
    w.push_back(workload_desc("TradeStatus",g_txn_workload_mix[5]/100.0,TxnTradeStatus));
  }
  if(g_txn_workload_mix[6]) {
    w.push_back(workload_desc("BrokerVolumn",g_txn_workload_mix[6]/100.0,TxnBrokerVolume));
  }
  if(g_txn_workload_mix[7]) {
    w.push_back(workload_desc("MarketFeed",g_txn_workload_mix[7]/100.0,TxnMarketFeed));
  }
  if(g_txn_workload_mix[8]) {
    w.push_back(workload_desc("MarketWatch",g_txn_workload_mix[8]/100.0,TxnMarketWatch));
  }
  if(g_txn_workload_mix[9]) {
    w.push_back(workload_desc("TradeUpdate",g_txn_workload_mix[9]/100.0,TxnTradeUpdate));
  }
  return w;
}

void TpceWorker::check_consistency() {
  fprintf(stdout,"worker %d check consistency\n",worker_id_);

  MemstoreUint64BPlusTree *trq = (MemstoreUint64BPlusTree *)(store_->stores_[TRADE_REQ]);
  MemstoreUint64BPlusTree *th  = (MemstoreUint64BPlusTree *)(store_->stores_[TRADE_HIST]);
  MemstoreUint64BPlusTree *trs = store_->_indexs[SEC_SC_TR];
  if(trq != NULL) {
  }
}
void TpceWorker::workload_report() {

}

void TpceWorker::thread_local_init() {

  tradeResultQueue = new std::queue<TTradeResultTxnInput> ();
  marketFeedQueue  = new std::queue<TTickerEntry> ();
  tradeResultCache = new TTradeResultTxnInput*[server_routine + 1];

  market_feed_process_idx = new int8_t[server_routine + 1];
  tickerBuffer = new TTickerEntry[server_routine + 1][10];

  //      processed_trades = new std::map<uint64_t,bool> ();

  type_margin_executed  = 0;
  num_remote_securities = 0;
  prev_executed = 0;
  prev_numbered = 0;

  for(uint i = 0;i < server_routine + 1;++i) {
    market_feed_process_idx[i] = -1;
    tradeResultCache[i] = NULL;
#ifdef RAD_TX
    txs_[i] = new DBRad(store_,worker_id_,rpc_,i);
#elif defined(OCC_TX)
    txs_[i] = new DBTX(store_,worker_id_,rpc_,i);
    remote_helper = new RemoteHelper(store_,total_partition,server_routine + 1);
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
}

/* TXs */
txn_result_t
TpceWorker::txn_trade_order(yield_func_t &yield) {

  TTradeOrderTxnInput input;
  /* Ensuring that the seed is the same as the abort seed */
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  static  int meta_size = store_->_schemas[HOLDING].meta_len;
  //assert(meta_size > 0 && meta_size == SI_META_LEN);

  bool    bExecutorIsAccountOwner;
  int32_t iTradeType;

  /* get input */
  input_generator_->GenerateTradeOrderInput(input, iTradeType,bExecutorIsAccountOwner);

  uint64_t seq(1);
  /* start tx */
  tx_->begin(db_logger_);

  uint64_t trade_id = get_last_trade_id();
  auto now_dts = CDateTime().GetDate();

  /* Frame 1 */
  customer_account::value *ca;
  seq = tx_->get(CUSTACCT,input.acct_id,(char **)(&ca),sizeof(customer_account::value));
#ifdef TX_DUMMY_WRITE
  tx_->write();
#endif
  assert(seq != 1);
  /* pre-execute the holding  summary TX to determine the remote read set */
  double hold_assets = 0.0;
  double acct_bal = 0.0;
  std::vector<uint64_t > remote_prices;

  std::map<std::string,std::pair<int,int> > remote_securities; remote_securities.clear();
  int r_idx(0); /* used to get remote entries */
  int pid;
  //      std::set<int> node_set;

  /* get customer */
  customers::value *c;
  seq = tx_->get(ECUST,ca->ca_c_id,(char **)(&c),sizeof(customers::value));
  assert(seq == 2);

  /* get broker */
  broker::value *b;
  //seq = tx_->get(BROKER,ca->ca_b_id,(char **)(&b),sizeof(broker::value));
  //assert(seq != 1);
#ifdef TX_DUMMY_WRITE
  // only reads the BNAME, so not necessary need this
  // tx_->write();
#endif

  /**** Frame 1 done ****/

  /* Frame 2 */
  // TODO!! since tax id is 14 byte fixed, so i just hard coded it here
  uint64_t ap_key = makeAPKey(input.acct_id, c->c_tax_id.data(),14);
  account_permission::value *ap;
  seq = tx_->get(ACCTPER,ap_key,(char **)(&ap),sizeof(account_permission::value));
  assert(seq == 2);

  trade_type::value *tt = TpceTradeHash[std::string(input.trade_type_id)];
  /**** Frame 2 done ****/

  /* Frame 3 */

  char exch_id[6 + 1]; /* 6 is from the specification */
  uint64_t symbol;
  uint64_t co_id;
#if 1

  if(input.symbol[0] == '\0') {
    /* securit symbol is not specified, just select security from company */
    co_id = CONameToId[input.co_name];

#ifdef SANITY_CHECKS
    {
      company::value *co_v;
      seq = tx_->get(COMPANY,co_id,(char **)(&co_v),sizeof(company::value));
      assert(seq == 2);
    }
#endif
    /* fetch security symbol, since security is replicated everywhere,
       so no distributed accesses here.
    */
    uint64_t sec_key = makeSecuritySecondIndex(co_id,input.issue);
    MemNode *mn     = store_->_indexs[SEC_IDX]->Get(sec_key);
    assert(mn != NULL);
    symbol = (uint64_t )(mn->value);

    security::value *sv;
    seq = tx_->get(SECURITY,symbol,(char **)(&sv),sizeof(security::value));
    assert(seq == 2);

    memcpy(exch_id,sv->s_ex_id.data(),sv->s_ex_id.size());

  } else {
    /* just directly fetch securtiy and company */
    symbol = makeSecurityIndex(input.symbol);
    security::value *sv;
    seq = tx_->get(SECURITY,(uint64_t)symbol,(char **)(&sv),sizeof(security::value));
    assert(seq == 2);
    memcpy(exch_id,sv->s_ex_id.data(),sv->s_ex_id.size());

    co_id = sv->s_co_id;

    company::value *co_v;
    seq = tx_->get(COMPANY,co_id,(char **)(&co_v),sizeof(company::value));
    assert(seq == 2);
  }
  //      fprintf(stdout,"trade order get symbol %s\n",(char *)symbol);
  //      sleep(1);
  auto hs_qty = 0;
  uint64_t hs_key = makeHSKey(input.acct_id,(const char *)symbol);
  holding_summary::value *hsv;
  seq = tx_->get(HOLDING_SUM,hs_key,(char **)(&hsv),sizeof(holding_summary::value));
  if(seq != 1) {
    /* find one */
    hs_qty = hsv->hs_qty;
  } else {
    //	assert(false);
  }

  if(input.type_is_margin) {
    type_margin_executed += 1;
    acct_bal = ca->ca_bal;
#ifdef RAD_TX
    RadIterator hs_iter((DBRad *)tx_,HOLDING_SUM, false);
#elif defined(OCC_TX)
    DBTXIterator hs_iter((DBTX *)tx_,HOLDING_SUM, false);
    //DBTXTempIterator hs_iter((DBTX *)tx_,HOLDING_SUM,false);
#elif defined(FARM)
    DBFarmIterator hs_iter((DBFarm *)tx_,HOLDING_SUM,false);
#elif defined(SI_TX)
    SIIterator hs_iter((DBSI *)tx_,HOLDING_SUM,false);
#else
    fprintf(stdout,"tx layer except rad is not implemented yet!\n");
    assert(false);
#endif
    char min_s = '\0';
    uint64_t hs_key_s = makeHSKey(input.acct_id,(char *)(&min_s));
    hs_iter.Seek(hs_key_s);
    while(hs_iter.Valid()) {

      uint64_t *key = (uint64_t *)(hs_iter.Key());
      if(key[0] != input.acct_id) break;
      holding_summary::value *v = (holding_summary::value *)((char *)hs_iter.Value() + meta_size);
      /* fetching from last trade */
      std::string s_symb = std::string((char *)(&(key[1])));

      uint64_t co_id = SecurityToCompany[s_symb];

      if( (pid = companyToPartition(co_id)) == current_partition)  {
        last_trade::value *ltv;
        seq = tx_->get(LT,(uint64_t)(&key[1]),(char **)(&ltv),sizeof(last_trade::value));
        assert(seq != 1);
#ifdef TX_DUMMY_WRITE
        /* local dummy write */
        tx_->write();
#endif
        //hold_assets += v->hs_qty * ltv->lt_price;
      } else {
        /* remote case, need fetch last trade, last trade price */
        r_idx = tx_->add_to_remote_set(LT,(uint64_t *)(s_symb.data()),s_symb.size(),pid);
#ifdef SANITY_CHECKS
        assert(remote_securities.find(s_symb) == remote_securities.end());
#endif
        //	    node_set.insert(pid);
        int hs_qty = v->hs_qty;
        remote_securities.insert(std::make_pair(s_symb,std::make_pair(r_idx,hs_qty)));
      }
      hs_iter.Next();
    }
    /* end type is margin */
  }

  /* flag on controling whether to do remote reads */
  int  num_servers(0);  r_idx = -1; int st_idx = -1; int tr_idx(-1);
  assert(SecurityToSector.find((char *)symbol) != SecurityToSector.end());
  uint64_t tr_key = makeTRKey(trade_id, ca->ca_b_id, (const char *)symbol,SecurityToSector[(char *)symbol].data());
  //      assert(decode_trade_routine(trade_id) == cor_id_ && decode_trade_payload(trade_id) < trade_id_);
  //      assert(processed_trades->find(trade_id) == processed_trades->end());
  uint64_t sec_s_t = makeSecondSecTrade((const char *)symbol,now_dts,trade_id);
  //      fprintf(stdout,"@%d make sec_s_t key time %lu, tid %lu\n",cor_id_,
  //      now_dts,decode_trade_payload(trade_id));
  uint64_t sec_sc_tr = makeSecondTradeRequest(SecurityToSector[(char *)symbol].data(),ca->ca_b_id,
                                              (const char *)symbol,
                                              trade_id);

  if( (pid = companyToPartition(co_id)) == current_partition) {
    /* i think this need to add a dummy write */
    last_trade::value *ltv;
    seq = tx_->get(LT,symbol,(char **)(&ltv),sizeof(last_trade::value));
    //	tx_->write();
    assert(seq != 1);
  } else {
    if(remote_securities.find((char *)symbol) == remote_securities.end()) {
      r_idx = tx_->add_to_remote_set(LT,(uint64_t *)symbol,2 * sizeof(uint64_t),pid);
    }
    /* remote insert indexs */
    st_idx=  tx_->remote_insert_idx(SEC_S_T,(uint64_t *)sec_s_t,5 * sizeof(uint64_t),pid);
    if(!tt->tt_is_mrkt) {
      /* need a local index insert? */
      tr_idx  = tx_->remote_insert(TRADE_REQ,(uint64_t *)tr_key, 4 * sizeof(uint64_t),pid);
      tx_->remote_insert_idx(SEC_SC_TR,(uint64_t *)sec_sc_tr,5 * sizeof(uint64_t),pid);
    }
  }

  if(remote_securities.size() > 0 || r_idx >= 0) {
    num_servers = tx_->do_remote_reads();
  } else {
    //fprintf(stdout,"no reads!\n");
    num_servers == 0;
  }

  /* fetch from trade type */
  trade_type::value *ttv = TpceTradeHash[input.trade_type_id];
#ifdef SANITY_CHECKS
  auto it = TpceTradeHash.find(input.trade_type_id);
  assert(it != TpceTradeHash.end());
#endif

  auto hold_qty = 0;
  auto hold_price = 0.0;
  auto buy_value = 0.0;
  auto sell_value = 0.0;
  auto needed_qty = input.trade_qty;
#if 1
  if(ttv->tt_is_sell) {
    if(hs_qty > 0) {
      std::vector<std::pair<int32_t, double>> hold_list;
      uint64_t min = makeHoldingKey(input.acct_id,0,(char *)symbol,0);
      uint64_t max = makeHoldingKey(input.acct_id,numeric_limits<uint64_t>::max(),(char *)symbol,
                                    numeric_limits<uint64_t>::max());

#ifdef RAD_TX
      RadIterator iter((DBRad *)tx_,HOLDING, false);
#elif  defined(OCC_TX)
      DBTXIterator iter((DBTX *)tx_,HOLDING,false);
      //DBTXTempIterator iter((DBTX *)tx_,HOLDING,false);
#elif  defined(FARM)
      DBFarmIterator iter((DBFarm *)tx_,HOLDING,false);
#elif  defined(SI_TX)
      SIIterator iter((DBSI *)tx_,HOLDING,false);
#else
      fprintf(stdout,"tx layer except rad is not implemented yet!\n");
      assert(false);
#endif
      //fprintf(stdout,"seek %lu %s\n",input.acct_id,(char *)symbol);
      iter.Seek(min);
      while(iter.Valid() && compareHoldingKey(iter.Key(),max)) {
        /* Fetch holding */
        holding::value *hv = (holding::value *)((char *)(iter.Value()) + meta_size);
#ifdef SANITY_CHECKS
        {
          /* this can be changed due to trade_result, so it is ok to relax this test */
#if 0
          holding::value *thv;
          seq = tx_->get(HOLDING,iter.Key(),(char **)(&thv),sizeof(holding::value));
          if(seq == 1) {
            printHoldingKey(iter.Key());
            assert(false);
          }
          assert(thv->h_qty == hv->h_qty);
          assert(thv->h_price == hv->h_price);
#endif
        }
#endif
        int32_t qty = hv->h_qty; double price = hv->h_price;
        hold_list.push_back( std::make_pair(qty,price) );
        iter.Next();
      }

      if(input.is_lifo) {
        std::reverse(hold_list.begin(),hold_list.end());
      }

      /* do some compution */
      for( auto& hold_list_cursor : hold_list ) {
        if(needed_qty == 0)
          break;
        hold_qty = hold_list_cursor.first;
        hold_price = hold_list_cursor.second;
        if( hold_qty > needed_qty ) {
          buy_value += needed_qty * hold_price;
          sell_value += needed_qty * input.requested_price;
          needed_qty = 0;
        } else {
          buy_value += hold_qty * hold_price;
          sell_value += hold_qty * input.requested_price;
          needed_qty -= hold_qty;
        }
      }
      /* end if hs_qty > 0*/
    }
  } else {
    /*
      This is a buy transaction, so estimate the impact to any currently held
      short positions in the security. These are represented as negative H_QTY
      holdings. Short postions will be covered before opening a long postion in this security.
    */
    if(hs_qty < 0) {
      std::vector<std::pair<int32_t, double>> hold_list;
      uint64_t min = makeHoldingKey(input.acct_id,0,(char *)symbol,0);
      uint64_t max = makeHoldingKey(input.acct_id,numeric_limits<uint64_t>::max(),(char *)symbol,
                                    numeric_limits<uint64_t>::max());

#ifdef RAD_TX
      RadIterator iter((DBRad *)tx_,HOLDING, false);
#elif defined(OCC_TX)
      DBTXIterator iter((DBTX *)tx_,HOLDING, false);
      //	  DBTXTempIterator iter((DBTX *)tx_,HOLDING, false);
#elif defined(FARM)
      DBFarmIterator iter((DBFarm *)tx_,HOLDING,false);
#elif defined(SI_TX)
      SIIterator iter((DBSI *)tx_,HOLDING,false);
#else
      fprintf(stdout,"tx layer except rad is not implemented yet!\n");
      assert(false);
#endif
      //fprintf(stdout,"seek %lu %s\n",input.acct_id,(char *)symbol);
      iter.Seek(min);
      while(iter.Valid() && compareHoldingKey(iter.Key(),max)) {
        /* Fetch holding */
        holding::value *hv = (holding::value *)((char *)(iter.Value()) + meta_size);
#ifdef SANITY_CHECKS
        {
          /* this can be changed due to trade_result, so it is ok to relax this test */
#if 0
          holding::value *thv;
          seq = tx_->get(HOLDING,iter.Key(),(char **)(&thv),sizeof(holding::value));
          if(seq == 1 ) {
            printHoldingKey(iter.Key());
            assert(false);
          }
          assert(thv->h_qty == hv->h_qty);
          assert(thv->h_price == hv->h_price);
#endif
        }
#endif
        int32_t qty = hv->h_qty; double price = hv->h_price;
        hold_list.push_back( std::make_pair(qty,price) );
        iter.Next();
      }
      if(input.is_lifo) {
        std::reverse(hold_list.begin(),hold_list.end());
      }

      /* do some compution */
      for( auto& hold_list_cursor : hold_list ) {

        if( needed_qty == 0)
          break;

        hold_qty = hold_list_cursor.first;
        hold_price = hold_list_cursor.second;

        if( hold_qty + needed_qty < 0 ) {
          sell_value += needed_qty * hold_price;
          buy_value += needed_qty * input.requested_price;
          needed_qty = 0;
        } else {
          hold_qty -= hold_qty;
          sell_value += hold_qty * hold_price;
          buy_value += hold_qty * input.requested_price;
          needed_qty -= hold_qty;
        }

      }
      /* end if hs_qty < 0*/
    }
  }
#endif
  //      fprintf(stdout,"calculate done\n");


  double comm_rate;
  auto tax_rates = 0.0;
  double charge_amount;
#if 1
  if(sell_value > buy_value && (ca->ca_tax_st == 1 || ca->ca_tax_st == 2)) {
    /* needs another iterator... */
#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,CUST_TAX, false);
#elif defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,CUST_TAX,false);
#elif defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,CUST_TAX,false);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,CUST_TAX,false);
#else
    fprintf(stdout,"tx layer except rad is not implemented yet!\n");
    assert(false);
#endif
    uint64_t start = makeCustTaxKey(ca->ca_c_id,0);
    iter.Seek(start);
    while(iter.Valid()) {
      uint64_t key = iter.Key();
      uint64_t c_id = key >> 32;
      if(c_id != ca->ca_c_id)
        break;
      else {
        tax_rates += TpceTaxMap[key & 0xffffffff];
#ifdef SANITY_CHECKS
        assert(TpceTaxMap.find(key & 0xffffffff) != TpceTaxMap.end());
#endif
      }
      iter.Next();
    }
  }

  /* commission rate, another iterator is needed TT */
  uint64_t cr_key = makeCRKey(c->c_tier,input.trade_type_id,exch_id,input.trade_qty);
#ifdef RAD_TX
  RadIterator cr_iter((DBRad *)tx_,CR, false);
#elif defined(OCC_TX)
  DBTXIterator cr_iter((DBTX *)tx_,CR, false);
#elif defined(FARM)
  DBFarmIterator cr_iter((DBFarm *)tx_,CR,false);
#elif defined(SI_TX)
  SIIterator cr_iter((DBSI *)tx_,CR,false);
#else
  fprintf(stdout,"tx layer except rad is not implemented yet!\n");
  assert(false);
#endif
  cr_iter.Seek(cr_key);
  if(cr_iter.Valid()) {
    uint64_t key = cr_iter.Key();
    if( ((uint64_t *) key)[3] < input.trade_qty) {
      fprintf(stdout,"expect %lu  real %lu\n",
              (uint64_t)(input.trade_qty),((uint64_t *)key)[3]);
      assert(false);
    }
    commission_rate::value *v = (commission_rate::value *)(cr_iter.Value() + meta_size);
    comm_rate = v->cr_rate;
  } else {
    assert(false);
  }

  /* fetch charge */
#ifdef SANITY_CHECKS
  assert(TpceTradeTypeMap.find(std::string(input.trade_type_id)) != TpceTradeTypeMap.end());
#endif

  uint64_t ch_key = makeChargeKey(TpceTradeTypeMap[std::string(input.trade_type_id)], c->c_tier);
  charge::value *charge_v;
  seq = tx_->get(CHARGE,ch_key,(char **)(&charge_v),sizeof(charge::value));
  charge_amount = charge_v->ch_chrg;
  assert(seq == 2);

  if(num_servers > 0) {
    indirect_yield(yield);
    tx_->get_remote_results(num_servers);

    /* re-execute */
    for(auto it = remote_securities.begin();it != remote_securities.end();++it) {
      auto r_v = it->second;
      last_trade::value *ltv;
      uint64_t seq = tx_->get_cached(r_v.first,(char **)(&ltv));
      if(unlikely(seq == 1))
        continue;
#ifdef TX_DUMMY_WRITE
      /* this is a dummy write */
      //fprintf(stdout,"write\n");
      assert(ltv != NULL);
      tx_->remote_write(r_v.first,(char *)ltv,sizeof(last_trade::value));
#endif
      hold_assets += ltv->lt_price * r_v.second;
    }
  }
#endif
#endif
  /**** Frame 3 done ****/
#if 1
  /* Frame 4 */
  /* Frame 4 mainly insert trade request */
  trade::value v_t;

  bool is_cash = !(input.type_is_margin);
  auto comm_amount = (comm_rate / 100) * input.trade_qty * input.requested_price;

  v_t.t_dts = now_dts;
  if(tt->tt_is_mrkt)
    v_t.t_st_id = std::string(input.st_submitted_id);
  else
    v_t.t_st_id = std::string(input.st_pending_id);
  v_t.t_tt_id = std::string(input.trade_type_id);
  v_t.t_is_cash = is_cash;
  v_t.t_s_symb = string((char *)symbol);
  v_t.t_qty = input.trade_qty;
  v_t.t_bid_price = input.requested_price;
  v_t.t_ca_id = input.acct_id;
  v_t.t_exec_name = std::string(input.exec_f_name) + " " + std::string(input.exec_l_name);
  v_t.t_trade_price = 0;
  v_t.t_chrg = charge_amount;
  v_t.t_comm = comm_amount;
  v_t.t_tax = 0;
  v_t.t_lifo = input.is_lifo;

  tx_->insert(TRADE,trade_id,(char *)(&v_t),sizeof(trade::value));
  // TODO!! may need some secondary indexes
  uint64_t trade_sec_ca = makeSecondCATrade(input.acct_id,now_dts,trade_id);
  tx_->insert_index(SEC_CA_TRADE,trade_sec_ca,new char[meta_size + sizeof(uint64_t)]);

  trade_request::value trv;
  if(!tt->tt_is_mrkt) {
    trv.tr_tt_id = std::string(input.trade_type_id);
    trv.tr_qty   = input.trade_qty;
    trv.tr_bid_price = input.requested_price;
    char *dummy = new char[sizeof(uint64_t) + meta_size];
    /* insert into trade request, this is sharded by symb  */
    if(companyToPartition(co_id) == current_partition) {
      /* local insert */
      //fprintf(stdout,"local insert\n");
      tx_->insert(TRADE_REQ,tr_key,(char *)(&trv),sizeof(trade_request::value));
      tx_->insert_index(SEC_SC_TR,sec_sc_tr,(char *)(dummy));
    } else {
      tx_->remote_write(tr_idx,(char *)(&trv),sizeof(trade_request::value));
      tx_->remote_write(tr_idx + 1,(char *)(&dummy),sizeof(uint64_t));
    }
  }

  /* trade history */
  trade_history::value th;
  uint64_t th_key = makeTHKey(trade_id,now_dts,"PNGD");
  //      th.th_st_id = v_t.t_st_id;
  tx_->insert(TRADE_HIST,th_key,(char *)(&th),sizeof(trade_history::value));

#ifdef TX_DUMMY_WRITE
  /* dummy write */
  if(r_idx != -1) {
    last_trade::value *ltv;
    auto seq = tx_->get_cached(r_idx,(char **)(&ltv));
    //fprintf(stdout,"write %d\n",r_idx);
    tx_->remote_write(r_idx,(char *)ltv,sizeof(last_trade::value));
  }
#endif
  /* insertions */
  if(pid != current_partition) {
    char *dummy = new char[meta_size + sizeof(uint64_t)];
    //fprintf(stdout,"remote insert idx\n",st_idx);
    tx_->remote_write(st_idx,(char *)(dummy),sizeof(uint64_t) + meta_size);
  } else {
    //	fprintf(stdout,"to: ");printSecondST(sec_s_t);
    tx_->insert_index(SEC_S_T,sec_s_t,new char[meta_size + sizeof(uint64_t)]);
  }
  /**** Frame 4 done ****/
#endif

  /* Frame x */
  /**** Frame x done ****/

  /* some cleaning */
  //      delete (uint64_t *)ap_key;
#ifdef RECORD
  assert(tx_->remoteset->get_broadcast_num() == num_servers);
  ntxn_remote_counts_ += tx_->remoteset->get_broadcast_num();
#endif
  bool res = tx_->end(yield);
  //      fprintf(stdout,"===========trade order end=============\n");
  if(!res) {
    //	trade_id_ -= 1; /* if abort, reset the next trade id */
  }
  else {
    /* sanity checks */
    /* submitting trade result */
    // FIXME!! the insertion shall be made in a timely manner
    tradeResultQueue->emplace(input.requested_price,trade_id);

    TTickerEntry entry;
    strcpy(entry.symbol,(char *)symbol);
    entry.trade_qty = input.trade_qty;
    entry.price_quote = input.requested_price;
    if(pid != current_partition) {
      /* prepare an RPC to insert the market feed */
#if 1
      char *msg = rpc_->get_fly_buf(cor_id_);
      memcpy(msg,(char *)(&entry),sizeof(TTickerEntry));
      rpc_->append_req(msg,RPC_ADD,sizeof(TTickerEntry),cor_id_,RRpc::REQ,pid);
#endif
    } else {
      /* directly insert to market feed */
      marketFeedQueue->push(entry);
    }

    /*
      TODO!!! If type is market, shall emplace a market feed entry
      else, it's a limit order
      but anyway the limit order will post the market entry here,
      so for simplicity i just put it here ;
      it seems that the specification does not add constraints, nice.

    */

    /* sanity checks */
    //	uint64_t *test = store_->GetIndex(SEC_S_T,sec_s_t);
    //	assert(test != NULL);
    //	uint64_t *dummy = store_->GetIndex(SEC_SC_TR,sec_sc_tr);
    //	assert(dummy != NULL);
    //	assert(false);
#if 0
    if(pid != current_partition && tr_idx != -1 ) {
      assert(pid == companyToPartition(co_id));
      tx_->begin();
      tx_->add_to_remote_set(TRADE_REQ,(uint64_t *)tr_key,4 * sizeof(uint64_t),pid);
      tx_->do_remote_reads();
      indirect_yield(yield);
      tx_->get_remote_results(1);
      trade_request::value *test;
      tx_->get_cached(0,(char **)(&test));
      fprintf(stdout,"trade qty %d, input %d\n",test->tr_qty,input.trade_qty);
      assert(test->tr_qty == input.trade_qty);
      assert(test->tr_bid_price == input.requested_price);
      fprintf(stdout,"assertion pass..\n");
      exit(-1);
    }
#endif
  }
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(res,tx_->remoteset->get_broadcast_num() + 1);
#else
  return txn_result_t(res,tx_->report_rw_set() + tx_->remoteset->read_items_ + tx_->remoteset->write_items_);
#endif
}

txn_result_t TpceWorker::txn_trade_result(yield_func_t &yield) {

  static  int meta_size = store_->_schemas[HOLDING].meta_len;
  if(tradeResultCache[cor_id_] == NULL) {
    if(unlikely(tradeResultQueue->empty())){
      return txn_result_t(true,1);
    }
    auto input = tradeResultQueue->front();
    tradeResultCache[cor_id_] = new TTradeResultTxnInput(input.trade_price,input.trade_id);

    tradeResultQueue->pop();
    //	assert(processed_trades->find(input.trade_id) == processed_trades->end());
  } else {
    //	fprintf(stdout,"fetch tid %lu %d aborted \n",
    //		decode_trade_payload(tradeResultCache[cor_id_]->trade_id),cor_id_);

  }
  TTradeResultTxnInput *input = (tradeResultCache[cor_id_]);
  //
  tx_->begin(db_logger_);
  uint64_t trade_dts = CDateTime().GetDate();

  /* Frame 1 */
  trade::value *tv;
  uint64_t seq = tx_->get(TRADE,input->trade_id,(char **)(&tv),sizeof(trade::value));

#ifdef SI_TX
  if(unlikely(seq == 1)){
    tx_->abort();
    return txn_result_t(false,0);
  }
#else

  ASSERT_PRINT(seq != 1,stdout,"trade id %lu\n",input->trade_id);
#endif
  auto acct_id = tv->t_ca_id;
  auto old_dts = tv->t_dts;


  /* fix ca->a tid */
  int pid = current_partition;
  if(trade_dts > old_dts) {
    /* fix ca->s tid */
    /* check for remote indexs */
    uint64_t sec_ca_t = makeSecondCATrade(acct_id,old_dts,input->trade_id);
    tx_->delete_index(SEC_CA_TRADE,sec_ca_t);
    ((uint64_t *)sec_ca_t)[1] = trade_dts;
    tx_->insert_index(SEC_CA_TRADE,sec_ca_t,new char[meta_size + sizeof(uint64_t)]);

#ifdef SANITY_CHECKS
    assert(SecurityToCompany.find(tv->t_s_symb.data()) != SecurityToCompany.end());
#endif
    uint64_t co_id = SecurityToCompany[tv->t_s_symb.data()];
    pid = companyToPartition(co_id);

    uint64_t sec_s_t = makeSecondSecTrade(tv->t_s_symb.data(),old_dts,input->trade_id);
    //fprintf(stdout,"tr pid %d\n",pid);
    if(pid == current_partition) {
      /* local case */
      //	  fprintf(stdout,"tr: ");printSecondST(sec_s_t);
      tx_->delete_index(SEC_S_T,sec_s_t);
      ((uint64_t *)sec_s_t)[3] = trade_dts;
      tx_->insert_index(SEC_S_T,sec_s_t,new char[meta_size + sizeof(uint64_t)]);
    } else {
      assert(decode_trade_worker(input->trade_id) == worker_id_);
      /*the first serve as a delete */

#if 1
      tx_->remote_read_idx(SEC_S_T,(uint64_t *)sec_s_t,5 * sizeof(uint64_t),pid);
      ((uint64_t *)sec_s_t)[3] = trade_dts;
      tx_->remote_insert_idx(SEC_S_T,(uint64_t *)sec_s_t,5 * sizeof(uint64_t),pid);
      tx_->do_remote_reads();
#endif
    }
  }

#if 1
  trade_type::value *tt = TpceTradeHash[tv->t_tt_id.data()];
#ifdef SANITY_CHECKS
  assert(TpceTradeHash.find(tv->t_tt_id.data()) != TpceTradeHash.end());
#endif

  auto hs_qty = 0;
  holding_summary::value *hsv;
  uint64_t hs_key = makeHSKey(acct_id,tv->t_s_symb.data());
  seq = tx_->get(HOLDING_SUM,hs_key,(char **)(&hsv),sizeof(holding_summary::value));
  if(seq == 1) {

  } else
    hs_qty = hsv->hs_qty;
#endif
  auto charge_v = tv->t_chrg;
  /**** Frame 1 done ****/
#if 1
  /* Frame 2 */
  auto buy_value = 0.0;
  auto sell_value = 0.0;
  auto needed_qty = tv->t_qty;
  uint64_t hold_id = 0;
  auto hold_price = 0;
  auto hold_qty = 0;

  customer_account::value *cav;
  seq = tx_->get(CUSTACCT,acct_id,(char **)(&cav),sizeof(customer_account::value));
  assert(seq != 1);
  /* a dummy write */
  //tx_->write(CUSTACCT,acct_id,(char *)cav,sizeof(customer_account::value));

  uint64_t min = makeHoldingKey(acct_id,0,(char *)(tv->t_s_symb.data()),0);
  uint64_t max = makeHoldingKey(acct_id,numeric_limits<uint64_t>::max(),
                                (char *)(tv->t_s_symb.data()),
                                numeric_limits<uint64_t>::max());

  /* possible insertion of holding */
  uint64_t hk = makeHoldingKey(acct_id,input->trade_id,tv->t_s_symb.data(),trade_dts);
#ifdef OCC_TX
  /* FIXME!, currently first get one here to avoid abort by itself */
  store_->stores_[HOLDING]->GetWithInsert(hk);
#endif
  if(tt->tt_is_sell) {
    /* sell */
    if(hs_qty == 0) {
      holding_summary::value hs;
      hs.hs_qty    = -1 * tv->t_qty;
      tx_->insert(HOLDING_SUM,hs_key,(char *)(&hs),sizeof(holding_summary::value));
    } else {
      if(hs_qty != tv->t_qty) {
        hsv->hs_qty = hs_qty - tv->t_qty;
        tx_->write(HOLDING_SUM,hs_key,(char *)hsv, sizeof(holding_summary::value));
      }
    }

    /* First look for existing holdings, H_QTY > 0 */
    if(hs_qty > 0) {

#ifdef RAD_TX
      RadIterator iter((DBRad *)tx_,HOLDING, false);
#elif defined(OCC_TX)
      DBTXIterator iter((DBTX *)tx_,HOLDING, false);
      //DBTXTempIterator iter((DBTX *)tx_,HOLDING,false);
#elif defined(FARM)
      DBFarmIterator iter((DBFarm *)tx_,HOLDING,false);
#elif defined(SI_TX)
      SIIterator iter((DBSI *)tx_,HOLDING,false);
#else
      fprintf(stdout,"tx layer except rad is not implemented yet!\n");
      assert(false);
#endif

      if(tv->t_lifo) {
        iter.Seek(min);

        while(iter.Valid() && compareHoldingKey(iter.Key(),max)) {
          if(needed_qty == 0) break;
          holding::value *hv;
          seq = tx_->get(HOLDING,iter.Key(),(char **)(&hv),sizeof(holding::value));
          if(seq == 1)  { iter.Next();continue;}
          hold_id = ((uint64_t *)(iter.Key()))[3];
          hold_qty = hv->h_qty;
          hold_price = hv->h_price;
#ifdef SANITY_CHECKS
          {
            trade::value *vht;
            seq = tx_->get(TRADE,hold_id,(char **)(&vht),sizeof(trade::value));
            assert(seq != 1);
          }
#endif
          uint64_t h_key = makeHoldingHistKey(input->trade_id,hold_id);
          if( hold_qty > needed_qty ) {

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = hold_qty - needed_qty;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            /* update with current holding cursor */
            hv->h_qty = hold_qty - needed_qty;
            tx_->write(HOLDING,iter.Key(),(char *)(hv),sizeof(holding::value));

            buy_value += needed_qty * hold_price;
            sell_value += needed_qty * input->trade_price;
            needed_qty = 0;

          } else {

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = 0;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            //		tx_->delete_(HOLDING,iter.Key());
            tx_->delete_by_node(HOLDING,iter.Node());
            buy_value += hold_qty * hold_price;
            sell_value += hold_qty * input->trade_price;
            needed_qty -= hold_qty;
          }
          iter.Next();
        }

      } else {
        //	    if(hsv->hs_qty == tv->t_qty) fprintf(stdout,"start one\n");
        iter.Seek(max);
        iter.Prev();
        while(iter.Valid() && compareHoldingKey(min,iter.Key())) {
          if(needed_qty == 0) break;
          //	      fprintf(stdout,"start ");
          //	      printHoldingKey(iter.Key());
          holding::value *hv;
          seq = tx_->get(HOLDING,iter.Key(),(char **)(&hv),sizeof(holding::value));
          if(seq == 1) {iter.Prev();continue; }
          hold_id = ((uint64_t *)(iter.Key()))[3];
          hold_qty = hv->h_qty;
          hold_price = hv->h_price;
#ifdef SANITY_CHECKS
          {
            trade::value *vht;
            seq = tx_->get(TRADE,hold_id,(char **)(&vht),sizeof(trade::value));
            assert(seq != 1);
          }
#endif
          uint64_t h_key = makeHoldingHistKey(input->trade_id,hold_id);
          if( hold_qty > needed_qty ) {

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = hold_qty - needed_qty;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            /* update with current holding cursor */
            hv->h_qty = hold_qty - needed_qty;
            tx_->write(HOLDING,iter.Key(),(char *)(hv),sizeof(holding::value));

            buy_value += needed_qty * hold_price;
            sell_value += needed_qty * input->trade_price;
            needed_qty = 0;

          } else {

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = 0;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            tx_->delete_by_node(HOLDING,iter.Node());

            buy_value += hold_qty * hold_price;
            sell_value += hold_qty * input->trade_price;
            needed_qty -= hold_qty;
          }
          iter.Prev();
        }
        /* end different order */
      }
    }
    if( needed_qty > 0) {
      holding_history::value v_hh;
      v_hh.hh_before_qty = 0;
      v_hh.hh_after_qty = -1 * needed_qty;
      uint64_t hh_key = makeHoldingHistKey(input->trade_id,input->trade_id);
      tx_->insert(HOLDING_HIST,hh_key,(char *)(&v_hh),sizeof(holding_history::value));

      holding::value v_h;
      v_h.h_price = input->trade_price;
      v_h.h_qty = -1 * needed_qty;
      //	  uint64_t hk = makeHoldingKey(acct_id,input.trade_id,tv->t_s_symb.data(),trade_dts);
      tx_->insert(HOLDING,hk,(char *)(&v_h),sizeof(holding::value));
    } else {
      /* rm holding summary if necessary */
      if( hs_qty == tv->t_qty){
        tx_->delete_(HOLDING_SUM,hs_key);
        //#ifdef SANITY_CHECKS

        bool flag = false;
        uint64_t prev_key = 0;
#ifdef RAD_TX
        RadIterator iter((DBRad *)tx_,HOLDING,false);
#elif defined(OCC_TX)
        DBTXIterator iter((DBTX *)tx_,HOLDING,false);
        //DBTXTempIterator iter((DBTX *)tx_,HOLDING,false);
#elif defined(FARM)
        DBFarmIterator iter((DBFarm *)tx_,HOLDING,false);
#elif defined(SI_TX)
        SIIterator iter((DBSI *)tx_,HOLDING,false);
#else
        assert(false);
#endif
        iter.Seek(min);
        // !! fixme ,maybe still need to delete
        while(iter.Valid() && compareHoldingKey(iter.Key(),max)) {
          holding::value *hv = (holding::value *)(iter.Value() + meta_size);
          tx_->delete_by_node(HOLDING,iter.Node());
          iter.Next();
        }
        /* end delete holding summary */
      }
    }
    /* end sell */
  } else  {
    /* buy */
    if(hs_qty == 0) {
      holding_summary::value hs;
      hs.hs_qty = tv->t_qty;
      tx_->insert(HOLDING_SUM,hs_key,(char *)(&hs),sizeof(holding_summary::value));
    } else {
      if(-1 * hold_qty != tv->t_qty) {
        hsv->hs_qty = tv->t_qty + hs_qty;
        tx_->write(HOLDING_SUM,hs_key,(char *)(&hsv),sizeof(holding_summary::value));
      }
    }

    if(hs_qty < 0) {
#ifdef RAD_TX
      RadIterator iter((DBRad *)tx_,HOLDING, false);
#elif defined(OCC_TX)
      DBTXIterator iter((DBTX *)tx_,HOLDING, false);
      //DBTXTempIterator iter((DBTX *)tx_,HOLDING,false);
#elif defined(FARM)
      DBFarmIterator iter((DBFarm *)tx_,HOLDING,false);
#elif defined(SI_TX)
      SIIterator iter((DBSI *)tx_,HOLDING,false);
#else
      fprintf(stdout,"tx layer except rad is not implemented yet!\n");
      assert(false);
#endif
      if(tv->t_lifo) {
        iter.Seek(min);

        while(iter.Valid() && compareHoldingKey(iter.Key(),max)) {
          if(needed_qty == 0) break;
          holding::value *hv;
          seq = tx_->get(HOLDING,iter.Key(),(char **)(&hv),sizeof(holding::value));
          if(seq == 1) { iter.Next(); continue;}
          hold_id = ((uint64_t *)(iter.Key()))[3];
          hold_qty = hv->h_qty;
          hold_price = hv->h_price;
#ifdef SANITY_CHECKS
          {
            //		trade::value *vht;
            //		seq = tx_->get(TRADE,hold_id,(char **)(&vht),sizeof(trade::value));
            //		assert(seq != 1);
          }
#endif
          uint64_t h_key = makeHoldingHistKey(input->trade_id,hold_id);
          if( hold_qty + needed_qty < 0) {

            /* update with current holding cursor */
            hv->h_qty = hold_qty + needed_qty;
            tx_->write();

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = hold_qty + needed_qty;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            //tx_->write(HOLDING,iter.Key(),(char *)(hv),sizeof(holding::value));

            buy_value += needed_qty * hold_price;
            sell_value += needed_qty * input->trade_price;
            needed_qty = 0;

          } else {

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = 0;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            tx_->delete_by_node(HOLDING,iter.Node());
            hold_qty *= -1;
            buy_value += hold_qty * hold_price;
            sell_value += hold_qty * input->trade_price;
            needed_qty -= hold_qty;
          }
          iter.Next();
        }

      } else {
        iter.Seek(max);
        iter.Prev();

        while(iter.Valid() && compareHoldingKey(min,iter.Key())) {
          if(needed_qty == 0) break;
          holding::value *hv;
          seq = tx_->get(HOLDING,iter.Key(),(char **)(&hv),sizeof(holding::value));
          if(seq == 1) { iter.Prev();continue;}
          hold_id = ((uint64_t *)(iter.Key()))[3];
          hold_qty = hv->h_qty;
          hold_price = hv->h_price;

          uint64_t h_key = makeHoldingHistKey(input->trade_id,hold_id);
          if( hold_qty + needed_qty < 0) {

            /* update with current holding cursor */
            hv->h_qty = hold_qty + needed_qty;
            //		tx_->write(HOLDING,iter.Key(),(char *)(hv),sizeof(holding::value));
            tx_->write();

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = hold_qty + needed_qty;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            buy_value += needed_qty * hold_price;
            sell_value += needed_qty * input->trade_price;
            needed_qty = 0;

          } else {

            holding_history::value v_hh;
            v_hh.hh_before_qty = hold_qty;
            v_hh.hh_after_qty = 0;
            tx_->insert(HOLDING_HIST,h_key,(char *)(&v_hh),sizeof(holding_history::value));

            tx_->delete_by_node(HOLDING,iter.Node());
            hold_qty *= -1;
            buy_value += hold_qty * hold_price;
            sell_value += hold_qty * input->trade_price;
            needed_qty -= hold_qty;
          }
          iter.Prev();
        }

      }
    }

    if( needed_qty > 0 ) {

      holding_history::value v_hh;
      v_hh.hh_before_qty = 0;
      v_hh.hh_after_qty  = needed_qty;
      uint64_t hh_key = makeHoldingHistKey(input->trade_id,input->trade_id);
      tx_->insert(HOLDING_HIST,hh_key,(char *)(&v_hh),sizeof(holding_history::value));

      holding::value v_h;
      v_h.h_price = input->trade_price;
      v_h.h_qty = needed_qty;

      tx_->insert(HOLDING,hk,(char *)(&v_h),sizeof(holding::value));
    } else {

      if(-1 * hold_qty == tv->t_qty) {
        tx_->delete_(HOLDING_SUM,hs_key);

#ifdef RAD_TX
        RadIterator iter((DBRad *)tx_,HOLDING,false);
#elif defined(OCC_TX)
        DBTXIterator iter((DBTX *)tx_,HOLDING,false);
        //DBTXTempIterator iter((DBTX *)tx_,HOLDING,false);
#elif defined(FARM)
        DBFarmIterator iter((DBFarm *)tx_,HOLDING,false);
#elif defined(SI_TX)
        SIIterator iter((DBSI *)tx_,HOLDING,false);
#endif

        /* delete forign key relationship */
        iter.Seek(min);
        // !! fixme ,maybe still need to delete
        while(iter.Valid() && compareHoldingKey(iter.Key(),max)) {
          //if(((DBRad *)tx_)->check_in_rwset(HOLDING,iter.Key()) == false) {
          //assert(false); TODO!! delete
          //  }
          //tx_->delete_(HOLDING,iter.Key());
          tx_->delete_by_node(HOLDING,iter.Node());
          iter.Next();
        }
        /* end delete holding summary */
      }
      /* end else delete check if there is an inserted requirements */
    }
    /* end buy */
  }
  /**** Frame 2 done ****/
#endif

#if 1
  /* Frame 3 */
  double tax_rates = 0.0;
  uint64_t cust_id = cav->ca_c_id;
  uint64_t ct_min = makeCustTaxKey(cust_id,0);

#ifdef RAD_TX
  RadIterator ct_iter((DBRad *)tx_,CUST_TAX, false);
#elif defined(OCC_TX)
  DBTXIterator ct_iter((DBTX *)tx_,CUST_TAX, false);
#elif defined(FARM)
  DBFarmIterator ct_iter((DBFarm *)tx_,CUST_TAX,false);
#elif defined(SI_TX)
  SIIterator ct_iter((DBSI *)tx_,CUST_TAX, false);
#else
  fprintf(stdout,"tx layer except rad is not implemented yet!\n");
  assert(false);
#endif
  ct_iter.Seek(ct_min);
  while(ct_iter.Valid()) {
    //	uint64_t *key = (uint64_t *)ct_iter.Key());
    if(ct_iter.Key() >> 32 != cust_id) break;

#ifdef SANITY_CHECKS
    {
      customer_taxrate::value *ctt;
      seq = tx_->get(CUST_TAX,ct_iter.Key(),(char **)(&ctt),sizeof(customer_taxrate::value));
      assert(seq == 2);
      assert(TpceTaxMap.find(ct_iter.Key() & 0xffffffff) != TpceTaxMap.end());
    }
#endif
    double tx_rate = TpceTaxMap[ct_iter.Key() & 0xffffffff];
    tax_rates += tx_rate;
    ct_iter.Next();
  }

  double tax_amount = (sell_value - buy_value ) * tax_rates;
  tv->t_tax = tax_amount;
  /**** Frame 3 done ****/
#endif

#if 1
  /* Frame 4 */
  /* commission rates */
  double comm_rate = 0.0;

  security::value *vs;
  seq = tx_->get(SECURITY,makeSecurityIndex(tv->t_s_symb.data()),(char **)(&vs),
                 sizeof(security::value));
  assert(seq == 2);

#if 1
  customers::value *vc;
  seq = tx_->get(ECUST,cust_id,(char **)(&vc),sizeof(customers::value));
  assert(seq == 2);

#ifdef RAD_TX
  RadIterator cr_iter((DBRad *)tx_,CR, false);
#elif defined(OCC_TX)
  DBTXIterator cr_iter((DBTX *)tx_,CR,false);
#elif defined(FARM)
  DBFarmIterator cr_iter((DBFarm *)tx_,CR,false);
#elif defined(SI_TX)
  SIIterator cr_iter((DBSI *)tx_,CR,false);
#else
  fprintf(stdout,"tx layer except rad is not implemented yet!\n");
  assert(false);
#endif

  uint64_t cr_min = makeCRKey(vc->c_tier,tv->t_tt_id.data(),vs->s_ex_id.data(),0);
  cr_iter.Seek(cr_min);
  while(cr_iter.Valid()) {
    uint64_t *key = (uint64_t *)cr_iter.Key();
    commission_rate::value *cv = (commission_rate::value *)(cr_iter.Value() + meta_size);
    if(cv->cr_to_qty < tv->t_qty) {
      cr_iter.Next();
      continue;
    } else {
      comm_rate = cv->cr_rate;
      break;
    }
  }
  assert(comm_rate != 0.0);
#endif
  /**** Frame 4 done ****/
#endif


#if 1
  double comm_amount = (comm_rate / 100) * (tv->t_qty * input->trade_price);
  /* Frame 5 */
  tv->t_comm        = comm_amount;
  tv->t_dts         = MAX(trade_dts,tv->t_dts);
  tv->t_st_id       = std::string("CMPT");
  //      assert(TpceStatusType.find(tv->t_st_id.data()) != TpceStatusType.end());
  tv->t_trade_price = input->trade_price;
  /* merge two trade write together */
  tx_->write(TRADE,input->trade_id,(char *)tv,sizeof(trade::value));

  trade_history::value v_th;
  uint64_t th_key = makeTHKey(input->trade_id,trade_dts,"CMPT");
  //      v_th.th_st_id = "CMPT";
  tx_->insert(TRADE_HIST,th_key,(char *)(&v_th),sizeof(trade_history::value));

  uint64_t b_id = cav->ca_b_id;
  //uint64_t b_id = 4300000014; // for test only
  if(brokerToPartition(b_id) != current_partition) {
    fprintf(stdout,"b_id %lu\n",cav->ca_b_id);
    assert(false);
  } else {
    broker::value *vb;
    seq = tx_->get(BROKER,b_id,(char **)(&vb),sizeof(broker::value));
    assert(seq != 1);
    vb->b_comm_total += comm_amount;
    vb->b_num_trades += 1;
    tx_->write(BROKER,b_id,(char *)vb,sizeof(broker::value));
  }
#endif
  /**** Frame 5 done ****/

#if 1
  /* Frame 6 */
  std::string cash_type;
  if(tv->t_is_cash) {
    cash_type = "Cash Account";
  } else {
    cash_type = "Margin";
  }

  CDateTime   due_date_time( trade_dts );
  due_date_time.Add(2, 0);        // add 2 days
  due_date_time.SetHMS(0,0,0,0);  // zero out time portion

  double se_amount = tv->t_qty * input->trade_price - charge_v  - comm_amount;

  settlement::value setv;
  setv.se_cash_type = cash_type;
  setv.se_cash_due_date = due_date_time.GetDate();
  setv.se_amt = se_amount;
  tx_->insert(SETTLEMENT,input->trade_id,(char *)(&setv),sizeof(settlement::value));

  if(tv->t_is_cash) {
    cav->ca_bal += setv.se_amt;

    cash_transaction::value ctx;
    ctx.ct_dts = trade_dts;
    ctx.ct_amt = se_amount;
    ctx.ct_name = std::string(tt->tt_name.data()) +
                  " " + std::to_string(tv->t_qty) + " shares of " + string(vs->s_name.data());
    tx_->insert(CASH_TX,input->trade_id,(char *)(&ctx),sizeof(cash_transaction::value));
  }
  /* a dummy write is needed */
#ifdef TX_DUMMY_WRITE
  tx_->write(CUSTACCT,acct_id,(char *)(cav),sizeof(customer_account::value));
#endif
  /**** Frame 6 done ****/
#endif

  if(pid != current_partition) {
    //	assert(false);
#if 1
    indirect_yield(yield);
    tx_->get_remote_results(1);
    tx_->remote_write(0,NULL,0); /* serve as a delete */
    char *dummy = new char[meta_size + sizeof(uint64_t)];
    tx_->remote_write(1,dummy,meta_size + sizeof(uint64_t));
#endif
  }
  //      fprintf(stdout,"trade res end\n");
#ifdef RECORD
  assert(tx_->remoteset->get_broadcast_num() == (pid != current_partition));
  ntxn_remote_counts_ += tx_->remoteset->get_broadcast_num();
#endif
  bool res = tx_->end(yield);

  /* Frame x */
  /**** Frame x done ****/
  if(res) {
    //if(1) {
    //	fprintf(stdout,"trade result commit  one..%d\n",cor_id_);
    //	sleep(1);
    //	tradeResultQueue->push(input);
    delete tradeResultCache[cor_id_];
    tradeResultCache[cor_id_] = NULL;
    //	assert(processed_trades->find(input->trade_id) == processed_trades->end());
    //	processed_trades->insert(std::make_pair(input->trade_id,true));
  } else {
    //	fprintf(stdout,"trade result abort one..%d\n",cor_id_);
  }
  //      else
  //	assert(false);
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(res,tx_->remoteset->get_broadcast_num() + 1);
#else
  return txn_result_t(res,tx_->report_rw_set() + tx_->remoteset->read_items_ + tx_->remoteset->write_items_);
#endif
}

txn_result_t TpceWorker::txn_broker_volume(yield_func_t &yield) {
  /* 5% of default execution */
  struct BrokerVolumnInput arg;
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  input_generator_->GenerateBrokerVolumeInput(arg.input);

  /* it only contains a single frame */
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif

  ForkSet fork_handler(rpc_,cor_id_);

  auto &sc_id = TpceSector[arg.input.sector_name];

  int num_servers(0);
#ifdef RAD_TX
  arg.time = ((DBRad *)tx_)->get_timestamp();
#endif

#ifdef SI_TX
  //#ifndef EM_OCC
  memcpy(arg.ts_vec,((DBSI *)(tx_))->ts_buffer_, sizeof(uint64_t) * total_partition);
  //#endif
#endif

#ifdef RAD_TX
  RadIterator r_iter((DBRad *)tx_,SEC_SC_CO,true);
#elif defined(OCC_TX)
  DBTXIterator r_iter((DBTX *)tx_,SEC_SC_CO,true);
#elif defined(FARM)
  DBFarmIterator r_iter((DBFarm *)tx_,SEC_SC_CO,true);
#elif defined(SI_TX)
  SIIterator r_iter((DBSI *)tx_,SEC_SC_CO,true);
#else
#endif
  int co_num(0);

  uint64_t sc_key = makeSecondSecCompany(sc_id.data(),0);
  r_iter.Seek(sc_key);
  while(r_iter.Valid()) {
    uint64_t *key = (uint64_t *)r_iter.Key();
    char *sc_id_k   = (char *)( &(key[0]));
    if(sc_id_k[0] != sc_id.data()[0] || sc_id_k[1] != sc_id.data()[1]) break;
    if(key[1] != current_partition)
      fork_handler.add(key[1]);
    co_num += 1;
    r_iter.Next();
  }
  delete (uint64_t *)sc_key;
  /* first observe companies */
  if(co_num > 1) {
    num_servers = fork_handler.fork(TX_BV,(char *)(&arg),sizeof(struct BrokerVolumnInput));
  }
  const char *min_s = "\0";
  /* doing the local parts */
  double   broker_volumn(0);
  for(uint i = 0;i < arg.input.broker_num;++i) {
    if(brokerToPartition(arg.input.broker_list[i]) != current_partition)
      continue;
#if 0
    broker::value bv;
    uint64_t seq = tx_->get_ro(BROKER,arg.input.broker_list[i],(char *)(&bv),yield);
    assert(seq != 1);
#endif

    int row_counts(0);
    uint64_t sec_s_key = makeSecondTradeRequest(sc_id.data(),arg.input.broker_list[i],
                                                min_s,0);
#ifdef RAD_TX
    RadIterator tr_iter((DBRad *)tx_,SEC_SC_TR,true);
#elif defined(OCC_TX)
    DBTXIterator tr_iter((DBTX *)tx_,SEC_SC_TR,true);
#elif defined(FARM)
    DBFarmIterator tr_iter((DBFarm *)tx_,SEC_SC_TR,true);
#elif defined(SI_TX)
    SIIterator tr_iter((DBSI *)tx_,SEC_SC_TR,true);
#else
#endif
    tr_iter.Seek(sec_s_key);
    while(tr_iter.Valid()) {
      uint64_t *key = (uint64_t *)(tr_iter.Key());
      if(key[1] != arg.input.broker_list[i]) break;
      uint64_t k = transferToTR(tr_iter.Key());
      trade_request::value v;
      //	  printSecondTRKey(tr_iter.Key());
      //	  printTRKey(k);
#if 1
      uint64_t seq = tx_->get_ro(TRADE_REQ,k,(char *)(&v),yield);
      if(unlikely(seq == 1)) { tr_iter.Next();continue; }

      broker_volumn += v.tr_bid_price * v.tr_qty;
#endif
      row_counts += 1;
      tr_iter.Next();
    }

  }
  uint16_t rwsize = 0;

  if(num_servers > 0) {
    indirect_yield(yield);
#ifdef OCC_TX
#ifdef OCC_RETRY
    fork_handler.reset();
    fork_handler.fork(RPC_R_VALIDATE,0);
    indirect_yield(yield);
    /* parse reply */
    uint16_t *reply_p = (uint16_t *)( fork_handler.reply_buf_);
#if OCC_RO_CHECK == 1
    for(uint i = 0;i < num_servers;++i) {
      //fprintf(stdout,"reply val %d\n",reply_p[i]);
      if(!reply_p[i]) {
        return txn_result_t(false,0);
      }
      //sleep(1);
#if PROFILE_RW_SET == 1
      rwsize += reply_p[i] - 1;
#endif
    }
#else
    //pass
#endif
          
#endif
#endif

  }
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret,fork_handler.get_server_num() + 1);
#else
  return txn_result_t(ret,rwsize + tx_->report_rw_set());
#endif
#endif
  return txn_result_t(true,broker_volumn);
}

txn_result_t TpceWorker::txn_customer_position(yield_func_t &yield) {

  TCustomerPositionTxnInput input;
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  input_generator_->GenerateCustomerPositionInput(input);
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif

  /* Frame 1 */
  /* find customer id */
  uint64_t cust_id = 0;
  if(input.cust_id == 0) {
    /* fetching */
    uint64_t sec_key = makeTaxCustKey(input.tax_id,0);
#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,SEC_TAX_CUST, true);
#elif  defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,SEC_TAX_CUST, true);
#elif  defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,SEC_TAX_CUST,true);
#elif  defined(SI_TX)
    SIIterator iter((DBSI *)tx_,SEC_TAX_CUST, true);
#else
    fprintf(stdout,"tx layer except rad is not implemented yet!\n");
    assert(false);
#endif
    iter.Seek(sec_key);
    if(iter.Valid()) {
      uint64_t *k = (uint64_t *)iter.Key();
      cust_id = k[2];
    } else {
      assert(false);
    }
  } else
    cust_id = input.cust_id;

  customers::value vc;

  uint64_t seq = tx_->get_ro(ECUST,cust_id,(char *)(&vc),yield);
  assert(seq != 1);

  uint64_t acct_ids[16];
  double   hold_assets[16];
  double   ca_bals[16];
  std::vector<int > remote_hs[16];
  memset(hold_assets,0,sizeof(double) * 16);
  int acct_len(0);
  /* calculating the holding summary */
#ifdef RAD_TX
  RadIterator iter((DBRad *)tx_,CUST_ACCT, false);
#elif defined(OCC_TX)
  DBTXIterator iter((DBTX *)tx_,CUST_ACCT,false);
#elif defined(FARM)
  DBFarmIterator iter((DBFarm *)tx_,CUST_ACCT,false);
#elif defined(SI_TX)
  SIIterator iter((DBSI *)tx_,CUST_ACCT, false);
#else
  fprintf(stdout,"tx layer except rad is not implemented yet!\n");
  assert(false);
#endif
  uint64_t c_key_start = makeCustAcctKey(cust_id,0);
  iter.Seek(c_key_start);
  while(iter.Valid()) {
    uint64_t *key = (uint64_t *)iter.Key();
    if(key[0] != cust_id) break;
    acct_ids[acct_len++] = key[1];
    iter.Next();
  }

  char min_s ='\0';
  assert(acct_len < 16);

  ForkSet fork_handler(rpc_,cor_id_);

  char input_buf[MAX_MSG_SIZE - 16 * sizeof(uint64_t)];
  memset(input_buf,0,MAX_MSG_SIZE - 16 * sizeof(uint64_t));
  struct CPInputHeader *c_header = (struct CPInputHeader *)input_buf;
  struct CPInputItem *items = (struct CPInputItem *)(input_buf + sizeof(struct CPInputHeader));
  int secs_count = 0;

  /* fetching from all the summaries */
  for(uint i = 0;i < acct_len;++i) {

    customer_account::value cav;
    seq = tx_->get_ro(CUSTACCT,acct_ids[i],(char *)(&cav),yield);

#ifdef RAD_TX
    RadIterator hs_iter((DBRad *)tx_,HOLDING_SUM, false);
#elif defined(OCC_TX)
    DBTXIterator hs_iter((DBTX *)tx_,HOLDING_SUM, false);
#elif defined(FARM)
    DBFarmIterator hs_iter((DBFarm *)tx_,HOLDING_SUM,false);
#elif defined(SI_TX)
    SIIterator hs_iter((DBSI *)tx_,HOLDING_SUM,false);
#else
    fprintf(stdout,"tx layer except rad is not implemented yet!\n");
    assert(false);
#endif
    uint64_t hs_start_key = makeHSKey(acct_ids[i],(const char *)(&min_s));
    hs_iter.Seek(hs_start_key);
    while(hs_iter.Valid()) {
      uint64_t *hs_key = (uint64_t *)(hs_iter.Key());
      if(hs_key[0] != acct_ids[i]) break;
      holding_summary::value hsv;
      seq = tx_->get_ro(HOLDING_SUM,hs_iter.Key(),(char *)(&hsv),yield);
      if(seq == 1) {hs_iter.Next();continue;}

      /* check for distributed accesses */
      uint64_t co_id = SecurityToCompany[(char *)(&hs_key[1])];
      int pid;
      if(( pid = companyToPartition(co_id)) != current_partition ) {
        //struct CPInputItem *p = (struct CPInputItem *)
        //	      fork_handler.add(pid,sizeof(struct CPInputItem));
        fork_handler.add(pid);
        //	    memset(items[secs_count].name,0,24);
        strcpy(items[secs_count].name,(char *)(&hs_key[1]));
        items[secs_count].idx0 = i;
        items[secs_count].idx1 = remote_hs[i].size();
        items[secs_count].pid  = pid;
        secs_count += 1;
        assert(i < 16);
        remote_hs[i].push_back(hsv.hs_qty);
      } else {
        /* fetching from local */
        last_trade::value lt;
        seq = tx_->get_ro(LT,(uint64_t )(&hs_key[1]),(char *)(&lt),yield);
        assert(seq != 1);
        hold_assets[i] += hsv.hs_qty * lt.lt_price;
      }

      hs_iter.Next();
    }

    ca_bals[i] = cav.ca_bal;
    /* end for */
  }
  int num_servers(0);
  assert(secs_count <= 256);
  c_header->secs = secs_count;

  if(c_header->secs > 0) {
#ifdef RAD_TX
    c_header->time = ((DBRad *)tx_)->get_timestamp();
#else
#endif
#ifdef SI_TX
    //#ifndef EM_OCC
    memcpy((c_header->ts_vec),((DBSI *)(tx_))->ts_buffer_, sizeof(uint64_t) * total_partition);
    //#endif
#endif
    num_servers = fork_handler.fork(TX_CP,input_buf, \
                                    sizeof(struct CPInputHeader ) \
                                    + sizeof(struct CPInputItem) * secs_count);
    //	num_servers = fork_handler.fork(TX_CP);
  }

  int hist_len = 0;
  /**** Frame 1 done ****/

#if 1
  if(input.get_history) {
    //	assert(false);
    /* execute frame 2 */
    /* Frame 2 start */
    assert(input.acct_id_idx < acct_len);
    uint64_t acct_id = acct_ids[input.acct_id_idx];

    /* from big to small */
    uint64_t max_ca_tid_sec = makeSecondCATrade(acct_id,std::numeric_limits<uint64_t>::max(),
                                                std::numeric_limits<uint64_t>::max());
#ifdef RAD_TX
    RadIterator ct_iter((DBRad *)tx_,SEC_CA_TRADE, true);
#elif defined(OCC_TX)
    DBTXIterator ct_iter((DBTX *)tx_,SEC_CA_TRADE, true);
#elif defined(FARM)
    DBFarmIterator ct_iter((DBFarm *)tx_,SEC_CA_TRADE,true);
#elif defined(SI_TX)
    SIIterator ct_iter((DBSI *)tx_,SEC_CA_TRADE,true);
#else
    fprintf(stdout,"tx layer except rad is not implemented yet!\n");
    assert(false);
#endif
    ct_iter.Seek(max_ca_tid_sec);
    ct_iter.Prev();
    const char *max_s = "ZZZZ";
    int count(0);
    while(ct_iter.Valid()) {
      uint64_t *key = (uint64_t *)(ct_iter.Key());
      if(count == 10 || key[0] != acct_id) break;
      //	  fprintf(stdout,"check tid %lu\n",key[2]);
      trade::value tv;
      seq = tx_->get_ro(TRADE,key[2],(char *)(&tv),yield);
      if(unlikely(seq == 1)) {
        ct_iter.Prev();
        continue;
      }
#ifdef RAD_TX
      RadIterator th_iter((DBRad *)tx_,TRADE_HIST, false);
#elif defined(OCC_TX)
      DBTXIterator th_iter((DBTX *)tx_,TRADE_HIST, false);
#elif defined(FARM)
      DBFarmIterator th_iter((DBFarm *)tx_,TRADE_HIST,false);
#elif defined(SI_TX)
      SIIterator th_iter((DBSI *)tx_,TRADE_HIST,false);
#else
      fprintf(stdout,"tx layer except rad is not implemented yet!\n");
      assert(false);
#endif

      uint64_t max_th_key = makeTHKey(key[2],std::numeric_limits<uint64_t>::max(),max_s);
      //uint64_t min_th_key = makeTHKey(key[2],0);
      th_iter.Seek(max_th_key);
      th_iter.Prev();

      while(th_iter.Valid()) {
        uint64_t *th_key = (uint64_t *)th_iter.Key();
        //	    fprintf(stdout,"check th hist %lu",th_key[0]);
        if(th_key[0] != key[2]) break;
        trade_history::value thv;
        seq = tx_->get_ro(TRADE_HIST,th_iter.Key(),(char *)(&thv),yield);
        if(unlikely(seq == 1)) { th_iter.Prev(); continue;}
        hist_len += 1;
        th_iter.Prev();
      }

      count += 1;
      ct_iter.Prev();
    }
    //	assert(false);
    if(hist_len < 10 || hist_len > max_hist_len) {
      /* according to the spec, this could not happen */
      /* it could happen at start */
      //	  fprintf(stdout,"get hist_len %d\n",hist_len);
      //	  assert(false);
    }
    //	fprintf(stdout,"get hist len %d\n",hist_len);

    /**** Frame 2 done ****/
  }
#endif
  //      sleep(1);
  /* fix remote results */
  if(num_servers > 0) {
    indirect_yield(yield);
    /* merge results */

    CPOutput*reply = (CPOutput *)fork_handler.get_reply();
    for(uint i = 0;i < c_header->secs; ++i) {
      //	  fprintf(stdout,"here %d %d || %f @%d\n",reply[i].idx0,reply[i].idx1, reply[i].val,cor_id_);
      int acct_idx = reply[i].idx0;
      int pos_idx  = reply[i].idx1;
      assert(reply[i].val != 0);
      if(acct_idx >= acct_len) {
        //	    fprintf(stdout,"idx %d, total %d, time %lu\n",i,c_header->secs,c_header->time);
        assert(false);
      }
      if(remote_hs[acct_idx].size() <= pos_idx) {
        assert(false);
      }
      hold_assets[acct_idx] += remote_hs[acct_idx][pos_idx] * reply[i].val;
    }
  }
  //      fprintf(stdout,"cp done\n");
  /* Frame x */
  /**** Frame x done ****/

#ifdef OCC_TX
  uint16_t rwsize = 0;
  if(num_servers > 0) {
#ifdef  OCC_RETRY
    fork_handler.reset();
    //	fork_handler.do_fork(1024);
    fork_handler.fork(RPC_R_VALIDATE,0);
    //	assert(rpc_->required_replies_ > 0);
    indirect_yield(yield);
    //	return txn_result_t(true,0);
    /* parse reply */
    uint16_t *reply_p = (uint16_t *)( fork_handler.reply_buf_);
#if OCC_RO_CHECK == 1
    for(uint i = 0;i < num_servers;++i) {
      //	  fprintf(stdout,"reply val %d\n",reply_p[i]);
      if(!reply_p[i]) {
        return txn_result_t(false,0);
      }
#if PROFILE_RW_SET == 1
      rwsize += reply_p[i] - 1;
#endif
    }
#endif
#endif
  }
  //      return txn_result_t(true,0);
  bool ret = ((DBTX *)tx_)->end_ro();
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret,fork_handler.get_server_num() + 1);
#else
  return txn_result_t(ret,rwsize + tx_->report_rw_set());
#endif
#endif
  return txn_result_t(true,hold_assets[0]);
}

txn_result_t TpceWorker::txn_market_watch(yield_func_t &yield) {

  TMarketWatchTxnInput input;
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  input_generator_->GenerateMarketWatchInput(input);

  uint64_t start_day =  CDateTime((TIMESTAMP_STRUCT*) (&(input.start_day))).GetDate();
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif
  ForkSet fork_handler(rpc_,cor_id_);

  int secs_count = 0;
  char input_buf[MAX_MSG_SIZE - 16 * sizeof(uint64_t)];
  memset(input_buf,0,MAX_MSG_SIZE - 16 * sizeof(uint64_t));
  MarketWatchInput *m_header = (MarketWatchInput *)input_buf;
  MWInputItem *items = (struct MWInputItem *)(input_buf + sizeof(MarketWatchInput));
  std::vector< SK > local_stock_list;

  const char *min_s = "\0";
  if(input.c_id) {
    //fprintf(stdout,"case 1\n");
    /* customer's watch list, easy case */
    auto &list = WatchItem[WatchList[input.c_id]];
    for(uint i = 0;i < list.size();++i) {
      auto &sec_key = list[i];
      int pid = companyToPartition(SecurityToCompany[sec_key]);

      if(pid == current_partition) {
        local_stock_list.emplace_back(sec_key);
      } else {
        /* add to forkset */
        fork_handler.add(pid);
        strcpy(items[secs_count].name,sec_key.data());
        items[secs_count].pid = pid;
        secs_count += 1;
      }
    }

  } else if (input.industry_name[0]) {
    //	fprintf(stdout,"case 2\n");
    uint64_t in_id = IndustryNametoID[input.industry_name];
    uint64_t in_co_s_s = makeSecondCompanyIndustrySecurity(in_id,
                                                           input.starting_co_id,min_s);
#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,SEC_SC_INS,true);
#elif defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,SEC_SC_INS,true);
#elif defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,SEC_SC_INS,true);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,SEC_SC_INS,true);
#else
    assert(false);
#endif
    int i_count = 0;
    iter.Seek(in_co_s_s);
    while(iter.Valid()) {
      uint64_t *key = (uint64_t *)(iter.Key());
      if(key[0] != in_id || key[1] > input.ending_co_id) break;
      char *sec_key = (char *)(&(key[2]));
      int pid = companyToPartition(SecurityToCompany[sec_key]);
      if(pid == current_partition) {
        local_stock_list.emplace_back(sec_key);
      } else {
        fork_handler.add(pid);
        strcpy(items[secs_count].name,sec_key);
        items[secs_count].pid = pid;
        secs_count += 1;
      }
      iter.Next();
    }
  } else if (input.acct_id) {
    //	fprintf(stdout,"case 3\n");
    uint64_t hs_s_key = makeHSKey(input.acct_id,min_s);
#ifdef RAD_TX
    RadIterator iter((DBRad *)tx_,HOLDING_SUM,false);
#elif defined(OCC_TX)
    DBTXIterator iter((DBTX *)tx_,HOLDING_SUM,false);
#elif defined(FARM)
    DBFarmIterator iter((DBFarm *)tx_,HOLDING_SUM,false);
#elif defined(SI_TX)
    SIIterator iter((DBSI *)tx_,HOLDING_SUM,false);
#else
    assert(false);
#endif

    iter.Seek(hs_s_key);
    while(iter.Valid()) {
      uint64_t *key = (uint64_t *)(iter.Key());
      if(key[0] != input.acct_id) break;
      char *sec_key = (char *)(&(key[1]));
      int pid = companyToPartition(SecurityToCompany[sec_key]);
      if(pid == current_partition) {
        local_stock_list.emplace_back(sec_key);
      } else {
        fork_handler.add(pid);
        strcpy(items[secs_count].name,sec_key);
        items[secs_count].pid = pid;
        secs_count += 1;
      }
      iter.Next();
    }
  }

  /* get stock list cursor done */
  if(unlikely(secs_count * sizeof(MWInputItem) + sizeof(MarketWatchInput)
              >= (MAX_MSG_SIZE - 16 * sizeof(uint64_t)))) {
    fprintf(stdout,"secs_count %d\n",secs_count);
    assert(false);
  }
  m_header->secs = secs_count;

  int num_servers(0);
  if(m_header->secs > 0) {
#ifdef RAD_TX
    m_header->time = ((DBRad *)tx_)->get_timestamp();
#endif

#ifdef SI_TX
    //#ifndef EM_OCC
    memcpy((m_header->ts_vec),((DBSI *)(tx_))->ts_buffer_, sizeof(uint64_t) * total_partition);
    //#endif
#endif
    m_header->start_day = start_day;
    num_servers = fork_handler.fork(TX_MW,(char *)input_buf,	\
                                    sizeof(MarketWatchInput) + sizeof(MWInputItem) * secs_count);
  }

  double old_mkt_cap = 0;
  double new_mkt_cap = 0;

  for(uint i = 0;i < local_stock_list.size();++i) {

    last_trade::value ltv;
    auto &s_key  = local_stock_list[i];
    uint64_t key = (uint64_t)(s_key.data);
    uint64_t seq = tx_->get_ro(LT,key,(char *)(&ltv),yield);
    assert(seq != 1);

    security::value sec;
    seq = tx_->get_ro(SECURITY,key,(char *)(&sec),yield);
    assert(seq != 1);
    uint64_t dm_key = makeDMKey(s_key.data,start_day);

    daily_market::value dm;
    seq = tx_->get_ro(DAILY_MARKET,dm_key,(char *)(&dm),yield);
    assert(seq != 1);
  }

  int rwsize = 0;

  if(num_servers > 0) {
    //fprintf(stdout,"fork to %d servers %d\n",num_servers,secs_count);
    indirect_yield(yield);
    //fprintf(stdout,"done\n");
    // TODO!! may collect the remote results
#ifdef OCC_TX
#ifdef OCC_RETRY
    fork_handler.reset();
    fork_handler.fork(RPC_R_VALIDATE,0);
    indirect_yield(yield);
    //	return txn_result_t(true,0);
    /* parse reply */
    uint16_t *reply_p = (uint16_t *)( fork_handler.reply_buf_);
#if OCC_RO_CHECK == 1
    for(uint i = 0;i < num_servers;++i) {
      //	  fprintf(stdout,"reply val %d\n",reply_p[i]);
      if(!reply_p[i]) {
        return txn_result_t(false,0);
      }
      rwsize += reply_p[i] - 1;
    }
#endif
#endif
#endif
  }
#ifdef OCC_TX
  //      return txn_result_t(true,0);
  bool ret = ((DBTX *)tx_)->end_ro();
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret,fork_handler.get_server_num() + 1);
#else
  return txn_result_t(ret,tx_->report_rw_set() + rwsize);
#endif
#else
  return txn_result_t(true,0);
#endif
}

txn_result_t TpceWorker::txn_security_detail(yield_func_t &yield) {

  TSecurityDetailTxnInput input;
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  input_generator_->GenerateSecurityDetailInput(input);
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif
  uint64_t co_id;

  /* getting a lot of security detail information */
  uint64_t sec_key = makeSecurityIndex(input.symbol);
  security::value s;
  uint64_t seq = tx_->get_ro(SECURITY,sec_key,(char *)(&s),yield);
  assert(seq != 1);
  /* Last trade need to be fetched remotely */
  int pid = companyToPartition(s.s_co_id);
  //ForkSet fork_handler(rpc_,cor_id_);
  char *req_buf   = msg_buf_alloctors[cor_id_].get_req_buf();
  char reply_buf[1024];
  if(pid != current_partition) {
    SEDInput r_input;
    memset((char *)(&r_input),0,sizeof(SEDInput));
    strcpy(r_input.name,input.symbol);
#ifdef RAD_TX
    r_input.time = ((DBRad *)tx_)->get_timestamp();
#endif
#ifdef SI_TX
    //#ifndef EM_OCC
    memcpy(r_input.ts_vec,((DBSI *)(tx_))->ts_buffer_, ts_manager->ts_size_);
    //#endif
#endif
    //fork_handler.add(pid);
    //fork_handler.fork(TX_SED,(char *)(&r_input),sizeof(SEDInput));
    memcpy(req_buf,(char *)(&r_input),sizeof(SEDInput));
    rpc_->prepare_multi_req(reply_buf,1,cor_id_);
    rpc_->append_req(req_buf,TX_SED,sizeof(SEDInput),cor_id_,RRpc::Y_REQ,pid);
  } else {
    /* fetch last price */
    last_trade ltv;
    seq = tx_->get_ro(LT,sec_key,(char *)(&ltv),yield);
    assert(seq != 1);
  }
  co_id = s.s_co_id;
#if 1
  company::value vc;
  seq = tx_->get_ro(COMPANY,s.s_co_id,(char *)(&vc),yield);
  assert(seq != 1);

  assert(TpceAddress.find(vc.co_ad_id) != TpceAddress.end());
  address::value *av = TpceAddress[vc.co_ad_id];

  assert(TpceExchangeMap.find(s.s_ex_id.data()) != TpceExchangeMap.end());
  int ex_id = TpceExchangeMap[s.s_ex_id.data()];
  exchange::value ex;
  seq = tx_->get_ro(EXCHANGE,ex_id,(char *)(&ex),yield);

  assert(TpceZipCode.find(av->ad_zc_code.data()) != TpceZipCode.end());
  zip_code::value *adz = TpceZipCode[av->ad_zc_code.data()];

  assert(TpceAddress.find(ex.ex_ad_id) != TpceAddress.end());
  address::value *ex_add = TpceAddress[ex.ex_ad_id];

  const char *min_s = "\0";
  uint64_t cc_start_key = makeCCKey(co_id,0,min_s);

#ifdef RAD_TX
  RadIterator cc_iter((DBRad *)tx_,COMPANY_C, false);
#elif defined(OCC_TX)
  DBTXIterator cc_iter((DBTX *)tx_,COMPANY_C,false);
#elif defined(FARM)
  DBFarmIterator cc_iter((DBFarm *)tx_,COMPANY_C,false);
#elif defined(SI_TX)
  SIIterator cc_iter((DBSI *)tx_,COMPANY_C,false);
#else
  assert(false);
#endif
  cc_iter.Seek(cc_start_key);
  while(cc_iter.Valid()) {
    uint64_t *key = (uint64_t *)cc_iter.Key();
    if(key[0] != co_id) break;

    company::value cc;
    seq = tx_->get_ro(COMPANY,key[1],(char *)(&cc),yield);
    char *industry_key = (char *)(&(key[2]));
    assert(TpceIndustry.find(industry_key) != TpceIndustry.end());
    industry::value *ind = TpceIndustry[industry_key];

    // TODO, may do some bookkeeping
    cc_iter.Next();
  }

  uint64_t fin_start_key = makeFinKey(co_id,0,0);
#ifdef RAD_TX
  RadIterator fin_iter((DBRad *)tx_,FINANCIAL, false);
#elif defined(OCC_TX)
  DBTXIterator fin_iter((DBTX *)tx_,FINANCIAL,false);
#elif defined(FARM)
  DBFarmIterator fin_iter((DBFarm *)tx_,FINANCIAL,false);
#elif defined(SI_TX)
  SIIterator fin_iter((DBSI *)tx_,FINANCIAL,false);
#else
  assert(false);
#endif
  int row_count(0);
  fin_iter.Seek(fin_start_key);
  while(fin_iter.Valid()) {
    uint64_t *key = (uint64_t *)fin_iter.Key();
    if(row_count >= max_fin_len || key[0] != co_id) break;

    financial::value fin;
    seq = tx_->get_ro(FINANCIAL,fin_iter.Key(),(char *)(&fin),yield);
    assert(seq != 1);
    row_count += 1;
    fin_iter.Next();
  }
  assert(row_count > 0);

  uint64_t dm_start_key = makeDMKey(input.symbol,
                                    CDateTime((TIMESTAMP_STRUCT*)&(input.start_day)).GetDate());
  assert(compareSecurityKey((uint64_t )(& (  ((uint64_t *)dm_start_key)[0] ) ),
                            (uint64_t)(input.symbol)) == true);

#ifdef RAD_TX
  RadIterator dm_iter((DBRad *)tx_,DAILY_MARKET, false);
#elif defined(OCC_TX)
  DBTXIterator dm_iter((DBTX *)tx_,DAILY_MARKET,false);
#elif defined(FARM)
  DBFarmIterator dm_iter((DBFarm *)tx_,DAILY_MARKET,false);
#elif defined(SI_TX)
  SIIterator dm_iter((DBSI *)tx_,DAILY_MARKET,false);
#else
  assert(false);
#endif
  dm_iter.Seek(dm_start_key);
  int dm_len(0);
  while(dm_iter.Valid()) {
    uint64_t *key = (uint64_t *)(dm_iter.Key());
    if(dm_len >= input.max_rows_to_return ||
       !compareSecurityKey( (uint64_t )(&(key[0])),(uint64_t)(input.symbol) ))
      break;
    daily_market::value dmv;
    seq = tx_->get_ro(DAILY_MARKET,dm_iter.Key(),(char *)(&dmv),yield);
    assert(seq != 1);
    dm_len += 1;
    dm_iter.Next();
  }
  assert(dm_len > 0);

  uint64_t newsx_s_key = makeNXRKey(co_id,0);
#ifdef RAD_TX
  RadIterator nx_iter((DBRad *)tx_,NEWS_XREF, false);
#elif defined(OCC_TX)
  DBTXIterator nx_iter((DBTX *)tx_,NEWS_XREF,false);
#elif defined(FARM)
  DBFarmIterator nx_iter((DBFarm *)tx_,NEWS_XREF,false);
#elif defined(SI_TX)
  SIIterator nx_iter((DBSI *)tx_,NEWS_XREF,false);
#else
  assert(false);
#endif
  nx_iter.Seek(newsx_s_key);
  int nx_len(0);
  while(nx_iter.Valid()) {
    uint64_t *key = (uint64_t *)nx_iter.Key();
    if(key[0] != co_id || nx_len >= max_news_len) break;

    assert(TpceNewsItem.find(key[1]) != TpceNewsItem.end());
    news_item::value *v = TpceNewsItem[key[1]];
    nx_len += 1;
    nx_iter.Next();
  }
#endif
  int rwsize = 0;
  if(pid != current_partition) {
    indirect_yield(yield);
#ifdef	OCC_TX
#ifdef  OCC_RETRY
    //fork_handler.reset();
    //fork_handler.fork(RPC_R_VALIDATE,0);
    rpc_->prepare_multi_req(reply_buf,1,cor_id_);
    rpc_->append_req(req_buf,RPC_R_VALIDATE,sizeof(SEDInput),cor_id_,RRpc::Y_REQ,pid);

    indirect_yield(yield);
    /* parse reply */
    //uint16_t *reply_p = (uint16_t *)( fork_handler.reply_buf_);
    uint16_t *reply_p = (uint16_t *)reply_buf;
#if OCC_RO_CHECK == 1
    if(!reply_p[0]) {
      return txn_result_t(false,0);
    }
    rwsize += reply_p[0] - 1;
#endif
#endif
    //	}

#endif
  } else
    msg_buf_alloctors[cor_id_].rollback_buf();

#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret,fork_handler.get_server_num() + 1);
#else
  return txn_result_t(ret,rwsize + tx_->report_rw_set());
#endif
#endif
  return txn_result_t(true,73);
}


txn_result_t TpceWorker::txn_trade_lookup(yield_func_t &yield) {

  TTradeLookupTxnInput input;
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  input_generator_->GenerateTradeLookupInput(input);
  if(input.frame_to_execute == 1) {
    return trade_lookup_frame1(yield,input);
  } else if(input.frame_to_execute == 2) {
    return trade_lookup_frame2(yield,input);
  } else if(input.frame_to_execute == 3) {
    return trade_lookup_frame3(yield,input);
  } else if(input.frame_to_execute == 4) {
    return trade_lookup_frame4(yield,input);
  } else
    assert(false);
  return txn_result_t(true,73);
}

txn_result_t TpceWorker::trade_lookup_frame1(yield_func_t &yield, TTradeLookupTxnInput &input) {
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif
  const char *min_s = "\0";
  for(uint i = 0;i < input.max_trades;++i) {

    /* This id is generated by egen, so we shall fix it.*/
    uint64_t tid = encode_trade_id(input.trade_id[i],0,0);
    trade::value trade;
    uint64_t seq = tx_->get_ro(TRADE,tid,(char *)(&trade),yield);
    if(unlikely(seq == 1)) {
      //	  fprintf(stdout,"%lu content %lu\n",decode_trade_mac(tid),decode_trade_payload(tid));
      //assert(seq != 1);
      continue;
    }
    trade_type::value *tt = TpceTradeHash[trade.t_tt_id.data()];

    /* retrive trade */
    settlement::value set;
    seq = tx_->get_ro(SETTLEMENT,tid,(char *)(&set),yield);
    assert(seq != 1);

    if(trade.t_is_cash) {
      cash_transaction::value ct;
      seq = tx_->get_ro(CASH_TX,tid,(char *)(&ct),yield);
#if 0
      if(seq == 1) {
        fprintf(stdout,"get tid %lu is cash %d\n",tid,trade.t_is_cash);
        fprintf(stdout,"sanity check trade comm %f name %s\n",trade.t_comm,
                trade.t_exec_name.data());
        assert(false);
      }
#else
      assert(seq != 1);
#endif
    }
    uint64_t th_start_key = makeTHKey(tid,0,min_s);

#ifdef RAD_TX
    RadIterator th_iter((DBRad *)tx_,TRADE_HIST,false);
#elif defined(OCC_TX)
    DBTXIterator th_iter((DBTX *)tx_,TRADE_HIST,false);
#elif defined(FARM)
    DBFarmIterator th_iter((DBFarm *)tx_,TRADE_HIST,false);
#elif defined(SI_TX)
    SIIterator th_iter((DBSI *)tx_,TRADE_HIST,false);
#else
    assert(false);
#endif
    int th_cursor(0);
    th_iter.Seek(th_start_key);
    while(th_iter.Valid()) {
      uint64_t *key = (uint64_t *)th_iter.Key();
      if(key[0] != tid || th_cursor >= TradeLookupMaxTradeHistoryRowsReturned) break;
      trade_history::value th;
      seq = tx_->get_ro(TRADE_HIST,th_iter.Key(),(char *)(&th),yield);
      assert(seq != 1);
      th_cursor += 1;

      th_iter.Next();
    }
  }
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(true,1);
#else
  return txn_result_t(true,tx_->report_rw_set());
#endif
}

txn_result_t TpceWorker::trade_lookup_frame2(yield_func_t &yield, TTradeLookupTxnInput &input) {
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif
  auto start_dts = CDateTime((TIMESTAMP_STRUCT*)&input.start_trade_dts).GetDate();
  auto end_dts = CDateTime((TIMESTAMP_STRUCT*)&input.end_trade_dts).GetDate();

  uint64_t t_ca_start_key =
      makeSecondCATrade(input.acct_id,
                        start_dts,
                        0);
  assert( caToPartition(input.acct_id) == current_partition);
  std::vector<uint64_t> found_trades; found_trades.reserve(input.max_trades);
  std::vector<bool> is_cash;  is_cash.reserve(input.max_trades);

#ifdef RAD_TX
  RadIterator t_ca_iter((DBRad *)tx_,SEC_CA_TRADE,true);
#elif defined(OCC_TX)
  DBTXIterator t_ca_iter((DBTX *)tx_,SEC_CA_TRADE,true);
#elif defined(FARM)
  DBFarmIterator t_ca_iter((DBFarm *)tx_,SEC_CA_TRADE,true);
#elif defined(SI_TX)
  SIIterator t_ca_iter((DBSI *)tx_,SEC_CA_TRADE,true);
#else
  assert(false);
#endif
  t_ca_iter.Seek(t_ca_start_key);
  int num_found(0);
  while(t_ca_iter.Valid()) {
    uint64_t *key = (uint64_t *)t_ca_iter.Key();
    if(num_found  > input.max_trades || key[0] != input.acct_id
       || key[1] > end_dts) break;

    found_trades.push_back(key[2]);
    trade::value trade;
    uint64_t seq = tx_->get_ro(TRADE,key[2],(char *)(&trade),yield);
    //	if(seq == 1) {
    //	  fprintf(stdout,"trade id %lu\n",key[2]);
    //	  assert(false);
    //	}
    assert(seq != 1);
    is_cash.push_back(trade.t_is_cash);

    num_found += 1;
    t_ca_iter.Next();
  }
#if 0
  /* according to the spec, num_found=0 could happen */
  if(!(num_found > 0 && num_found <= input.max_trades)) {
    fprintf(stdout,"num found %d\n",num_found);
    fprintf(stdout,"acctid %lu, dts %lu\n",input.acct_id,start_dts);
    assert(false);
  }
#endif
  //      fprintf(stdout,"num found %d\n",num_found);
  //      sleep(1);
  const char *min_s = "\0";
  for(uint i = 0;i < num_found;++i) {
    settlement::value set;
    uint64_t seq = tx_->get_ro(SETTLEMENT,found_trades[i],(char *)(&set),yield);
#if 0
    if(seq == 1) {
      fprintf(stdout,"trade id %lu\n",found_trades[i]);
      assert(false);
    }
#else
    assert(seq != 1);
#endif
    if(is_cash[i]) {
      cash_transaction::value ct;
      seq = tx_->get_ro(CASH_TX,found_trades[i],(char *)(&ct),yield);
#if 0
      if(seq == 1) {
        fprintf(stdout,"trade id %lu\n",found_trades[i]);
        assert(false);
      }
#else
      assert(seq != 1);
#endif
    }
    uint64_t th_start_key = makeTHKey(found_trades[i],0,min_s);

#ifdef RAD_TX
    RadIterator th_iter((DBRad *)tx_,TRADE_HIST,false);
#elif defined(OCC_TX)
    DBTXIterator th_iter((DBTX *)tx_,TRADE_HIST,false);
#elif defined(FARM)
    DBFarmIterator th_iter((DBFarm *)tx_,TRADE_HIST,false);
#elif defined(SI_TX)
    SIIterator th_iter((DBSI *)tx_,TRADE_HIST,false);
#else
    assert(false);
#endif
    int th_cursor(0);
    th_iter.Seek(th_start_key);
    while(th_iter.Valid()) {
      uint64_t *key = (uint64_t *)th_iter.Key();
      if(key[0] != found_trades[i]
         || th_cursor >= TradeLookupMaxTradeHistoryRowsReturned) break;
      trade_history::value th;
      seq = tx_->get_ro(TRADE_HIST,th_iter.Key(),(char *)(&th),yield);
      assert(seq != 1);
      th_cursor += 1;
      th_iter.Next();
    }
  }
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret,1);
#else
  return txn_result_t(ret,tx_->report_rw_set());
#endif
#endif
  return txn_result_t(true,73);
}

txn_result_t TpceWorker::trade_lookup_frame3(yield_func_t &yield, TTradeLookupTxnInput &input) {
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif
  //      uint64_t sec_index = makeSecurityIndex(input.symbol);
  uint64_t co_id = SecurityToCompany[input.symbol];
  int pid = companyToPartition(co_id);

  ForkSet fork_handler(rpc_,cor_id_);

  uint64_t start_dts = CDateTime((TIMESTAMP_STRUCT*)&input.start_trade_dts).GetDate();
  uint64_t end_dts = CDateTime((TIMESTAMP_STRUCT*)&input.end_trade_dts).GetDate();

  TradeLookupOutput1 out;
  TradeLookupOutput1 *second_input;

  if(pid != current_partition) {
    /* slow path */
    TradeLookupInput t_input;
    memset((char *)(&t_input),0,sizeof(TradeLookupInput));
    strcpy(t_input.name,input.symbol);
    t_input.start_dts = start_dts;
    t_input.end_dts   = end_dts;
#ifdef RAD_TX
    t_input.time = ((DBRad *)tx_)->get_timestamp();
#endif

#ifdef SI_TX
    //#ifndef EM_OCC
    memcpy((t_input.ts_vec),((DBSI *)(tx_))->ts_buffer_, ts_manager->ts_size_);
    //#endif
#endif
    fork_handler.add(pid);
    //	fprintf(stdout,"fork %s\n",t_input.name);
    fork_handler.fork(TX_TL0,(char *)(&t_input),sizeof(TradeLookupInput));
    indirect_yield(yield);

    second_input = (TradeLookupOutput1 *)fork_handler.get_reply();
  } else {
    /* local case */
    uint64_t sec_s_key = makeSecondSecTrade(input.symbol,start_dts,0);
#ifdef RAD_TX
    RadIterator sec_iter((DBRad *)tx_,SEC_S_T,true);
#elif defined(OCC_TX)
    DBTXIterator sec_iter((DBTX *)tx_,SEC_S_T,true);
#elif defined(FARM)
    DBFarmIterator sec_iter((DBFarm *)tx_,SEC_S_T,true);
#elif defined(SI_TX)
    SIIterator sec_iter((DBSI *)tx_,SEC_S_T,true);
#else
    /* not implemented yet */
    assert(false);
#endif
    sec_iter.Seek(sec_s_key);
    int num_found(0);
    while(sec_iter.Valid()) {
      uint64_t *key = (uint64_t *)sec_iter.Key();
      if(num_found >= TradeLookupMaxRows ||
         !compareSecurityKey((uint64_t ) (&key[0]), (uint64_t)(input.symbol))
         || key[3] > end_dts)
        break;

      out.trade_ids[num_found++] = key[4];
      sec_iter.Next();
    }
    //	assert(num_found > 0);
    assert(num_found <= 256);
    out.count = num_found;
    second_input = &out;
  }
  //      fprintf(stdout,"count %d\n",second_input->count);
  //      sleep(1);
  assert(second_input->count >= 0 && second_input->count <= TradeLookupMaxRows);
  fork_handler.reset();
  std::vector<uint64_t> local_trades;
  for(uint i = 0;i < second_input->count;++i) {
    int pid = decode_trade_mac(second_input->trade_ids[i]);
    if(pid != current_partition)
      fork_handler.add(pid);
    else
      local_trades.push_back(second_input->trade_ids[i]);
  }
  int num_servers(0);
  if(local_trades.size() < second_input->count) {
    /* again fork */
#ifdef RAD_TX
    second_input->time = ((DBRad *)tx_)->get_timestamp();
#endif
#ifdef SI_TX
    //#ifndef EM_OCC
    memcpy((second_input->ts_vec),((DBSI *)(tx_))->ts_buffer_, ts_manager->ts_size_);
    //#endif
#endif
    num_servers = fork_handler.fork(TX_TL1,(char *)(second_input),sizeof(TradeLookupOutput1));
  }

  const char *min_s = "\0";
  /* do local parts */
  for(uint i = 0;i < local_trades.size();++i) {
    uint64_t tid = local_trades[i];
    settlement::value set;
    uint64_t seq = tx_->get_ro(SETTLEMENT,tid,(char *)(&set),yield);
    //	if(seq == 1) {
    //	  fprintf(stdout,"tid %lu\n",tid);
    assert(seq != 1);
    //	}
    trade::value tv;
    seq = tx_->get_ro(TRADE,tid,(char *)(&tv),yield);
    assert(seq != 1);
    if(tv.t_is_cash) {
      cash_transaction::value ct;
      seq = tx_->get_ro(CASH_TX,tid,(char *)(&ct),yield);
      assert(seq != 1);
    }

    uint64_t th_start_key = makeTHKey(tid,0,min_s);
#ifdef RAD_TX
    RadIterator th_iter((DBRad *)tx_,TRADE_HIST,false);
#elif defined(OCC_TX)
    DBTXIterator th_iter((DBTX *)tx_,TRADE_HIST,false);
#elif defined(FARM)
    DBFarmIterator th_iter((DBFarm *)tx_,TRADE_HIST,false);
#elif defined(SI_TX)
    SIIterator th_iter((DBSI *)tx_,TRADE_HIST,false);
#else
    assert(false);
#endif
    int th_cursor(0);
    th_iter.Seek(th_start_key);
    while(th_iter.Valid()) {
      uint64_t *key = (uint64_t *)th_iter.Key();
      if(key[0] != tid
         || th_cursor >= TradeLookupMaxTradeHistoryRowsReturned) break;
      trade_history::value th;
      seq = tx_->get_ro(TRADE_HIST,th_iter.Key(),(char *)(&th),yield);
      assert(seq != 1);

      th_cursor += 1;
      th_iter.Next();
    }
  }
  int rwsize = 0;
  if(num_servers > 0) {
    indirect_yield(yield);
    // TODO!! occ shall make re-check
#ifdef OCC_TX
#ifdef OCC_RETRY
    fork_handler.reset();
    //	fork_handler.do_fork(1024);
    fork_handler.fork(RPC_R_VALIDATE,0);
    indirect_yield(yield);
    /* parse reply */
    uint16_t *reply_p = (uint16_t *)( fork_handler.reply_buf_);
#if OCC_RO_CHECK == 1
    for(uint i = 0;i < num_servers;++i) {
      //	  fprintf(stdout,"reply val %d\n",reply_p[i]);
      if(!reply_p[i]) {
        return txn_result_t(false,0);
      }
      rwsize += reply_p[i] - 1;
    }
#endif
#endif
#endif

  }
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret,fork_handler.get_server_num() + 1);
#else
  return txn_result_t(ret,tx_->report_rw_set() + rwsize);
#endif
#endif
  //      assert(false);
  return txn_result_t(true,73);
}

txn_result_t TpceWorker::trade_lookup_frame4(yield_func_t &yield, TTradeLookupTxnInput &input) {
  //      assert(false);
#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif
  auto dts = CDateTime((TIMESTAMP_STRUCT*)&(input.start_trade_dts)).GetDate();
  uint64_t sec_ca_t_key = makeSecondCATrade(input.acct_id,dts,0);
#ifdef RAD_TX
  RadIterator t_iter((DBRad *)tx_,SEC_CA_TRADE,true);
#elif defined(OCC_TX)
  DBTXIterator t_iter((DBTX *)tx_,SEC_CA_TRADE,true);
#elif defined(FARM)
  DBFarmIterator t_iter((DBFarm *)tx_,SEC_CA_TRADE,true);
#elif defined(SI_TX)
  SIIterator t_iter((DBSI *)tx_,SEC_CA_TRADE,true);
#else
  assert(false);
#endif

  t_iter.Seek(sec_ca_t_key);
  if(t_iter.Valid()) {
    uint64_t *key = (uint64_t *)t_iter.Key();
    if(key[0] != input.acct_id) goto TL4_END;
    uint64_t tid = key[2];
    int pid = decode_trade_mac(tid);
    uint64_t payload = decode_trade_payload(tid);
    if(pid == current_partition) {
      /* fetching  trade history */
#ifdef RAD_TX
      RadIterator hh_iter((DBRad *)tx_,HOLDING_HIST,false); /* holding history */
#elif defined(OCC_TX)
      DBTXIterator hh_iter((DBTX *)tx_,HOLDING_HIST,false);
#elif defined(FARM)
      DBFarmIterator hh_iter((DBFarm *)tx_,HOLDING_HIST,false);
#elif defined(SI_TX)
      SIIterator hh_iter((DBSI *)tx_,HOLDING_HIST,false);
#else
      assert(false);
#endif

      uint64_t th_start_key = makeHoldingHistKey(tid,0);
      hh_iter.Seek(th_start_key);
      int count(0);
      while(hh_iter.Valid()) {

        uint64_t *key = (uint64_t *)hh_iter.Key();
        if(key[0] != tid || count >= TradeLookupFrame4MaxRows)
          break;
        /* fetching holding history */
        holding_history::value hhv;
        uint64_t seq = tx_->get_ro(HOLDING_HIST,hh_iter.Key(),(char *)(&hhv),yield);
        if(unlikely(seq == 1)) { hh_iter.Next();continue;};

        count += 1;
        hh_iter.Next();
      }
    } else {
      // it's not possible
      fprintf(stdout,
              "[TRADE_LOOKUP4] remote trade fetch, currently not implemented,id %d\n",pid);
      assert(false);
    }
  }
TL4_END:
#ifdef OCC_TX
  bool ret = ((DBTX *)tx_)->end_ro();
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret,1);
#else
  return txn_result_t(ret,tx_->report_rw_set());
#endif
#endif
  return txn_result_t(true,73);
}

txn_result_t TpceWorker::txn_trade_status(yield_func_t &yield) {

  TTradeStatusTxnInput input;
  input_generator_->GenerateTradeStatusInput(input);
  assert(caToPartition(input.acct_id) == current_partition);

#ifdef OCC_TX
  ((DBTX *)tx_)->local_ro_begin();
#elif defined(SI_TX)
  ((DBSI *)tx_)->local_ro_begin();
#else
  tx_->begin();
#endif

  uint64_t t_ca_start = makeSecondCATrade(input.acct_id,0,0);
  int t_cursor(0);

#ifdef RAD_TX
  RadIterator ca_iter((DBRad *)tx_,SEC_CA_TRADE,true);
#elif defined(OCC_TX)
  DBTXIterator ca_iter((DBTX *)tx_,SEC_CA_TRADE,true);
#elif defined(FARM)
  DBFarmIterator ca_iter((DBFarm *)tx_,SEC_CA_TRADE,true);
#elif defined(SI_TX)
  SIIterator ca_iter((DBSI *)tx_,SEC_CA_TRADE,true);
#else
  assert(false);
#endif
  ca_iter.Seek(t_ca_start);

  while(ca_iter.Valid()) {
    uint64_t *key = (uint64_t *)ca_iter.Key();
    if(key[0] != input.acct_id || t_cursor >= 50)
      break;
    trade::value tr;
    uint64_t seq = tx_->get_ro(TRADE,key[2],(char *)(&tr),yield);
    //assert(seq != 1);
    if(unlikely(seq == 1)) { ca_iter.Next();continue; }
    assert(TpceStatusType.find(tr.t_st_id.data()) != TpceStatusType.end());
    status_type::value *st = TpceStatusType[tr.t_st_id.data()];
    assert(TpceTradeHash.find(tr.t_tt_id.data()) != TpceTradeHash.end());
    trade_type::value  *tt = TpceTradeHash[tr.t_tt_id.data()];

    security::value sec;
    uint64_t sec_key = makeSecurityIndex(tr.t_s_symb.data());
    seq = tx_->get_ro(SECURITY,sec_key,(char *)(&sec),yield);
    assert(seq != 1);
    delete (uint64_t *)sec_key;

    exchange::value exc;
    uint64_t ex_id = TpceExchangeMap[sec.s_ex_id.data()];
    seq = tx_->get_ro(EXCHANGE,ex_id,(char *)(&exc),yield);
    assert(seq != 1);

    t_cursor += 1;
    ca_iter.Next();
  }

  customer_account::value ca;
  uint64_t seq = tx_->get_ro(CUSTACCT,input.acct_id,(char *)(&ca),yield);
  assert(seq != 1);
  customers::value cust;
  seq = tx_->get_ro(ECUST,ca.ca_c_id,(char *)(&cust),yield);
  assert(seq != 1);
  broker::value b;
  seq = tx_->get_ro(BROKER,ca.ca_b_id,(char *)(&b),yield);
  assert(seq != 1);
#ifdef OCC_TX
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(((DBTX *)tx_)->end_ro(),1);
#else
  return txn_result_t(((DBTX *)tx_)->end_ro(),tx_->report_rw_set());
#endif
#endif
  return txn_result_t(true,73);
}

txn_result_t TpceWorker::txn_market_feed(yield_func_t &yield) {

  static  int meta_size = store_->_schemas[HOLDING].meta_len;
  /* prepare indexes */
  if(market_feed_process_idx[cor_id_] == -1) {
    /* nothing has processed yet*/
    if(marketFeedQueue->size() < 10)
      return txn_result_t(true,-1);
    for(uint i = 0;i < 10;++i) {
      tickerBuffer[cor_id_][i] = marketFeedQueue->front();
      marketFeedQueue->pop();
      //	  assert(marketFeedQueue->size() < 4096);
    }
  }
  auto now_dts = CDateTime().GetDate();
  //      TStatusAndTradeType type = StatusAndTradeType;
  //      fprintf(stdout,"market feed process dts %lu @%d\n",now_dts,cor_id_);
  // currently we only have a simple MEE implementation, so just process 10 entries
  //      assert(max_feed_len == 10);
  int rwsize = 0;
  int processed_count = 0;

  for(uint i = market_feed_process_idx[cor_id_] + 1;i < 10;++i) {
    // according to the spec, each feed is a seperate tx
    // although some work treat it as a big transaction, is is not necessary
#if 1
    tx_->begin(db_logger_);
    std::vector<uint64_t> local_trades;
    int num_remote_trades(0);

    auto &ticker = tickerBuffer[cor_id_][i];
    assert(SecurityToCompany.find(ticker.symbol) != SecurityToCompany.end());

    uint64_t sk = makeSecurityIndex(ticker.symbol);
    last_trade::value *lt;
    uint64_t seq = tx_->get(LT,sk,(char **)(&lt),sizeof(last_trade::value));
    assert(seq != 1);

    lt->lt_price = lt->lt_price + ticker.price_quote;
    lt->lt_vol   = ticker.price_quote;
    lt->lt_dts = now_dts;
    tx_->write(); /* write the last read value */

#ifdef RAD_TX
    RadIterator tr_iter((DBRad *)tx_,TRADE_REQ,false);
#elif  defined(OCC_TX)
    DBTXTempIterator tr_iter((DBTX *)tx_,TRADE_REQ,false);
#elif  defined(FARM)
    DBFarmIterator tr_iter((DBFarm *)tx_,TRADE_REQ,false);
#elif  defined(SI_TX)
    SIIterator tr_iter((DBSI *)tx_,TRADE_REQ,false);
#else
    assert(false);
#endif
    const char *min_s = "\0";
    uint64_t tr_start_key = makeTRKey(0,0,ticker.symbol,min_s);
    tr_iter.Seek(tr_start_key);
    int tr_len = 0;
    //fprintf(stdout,"[FEED], fetch key %s\n",ticker.symbol);
    //          set<uint64_t> remote_trades;
#if 1
    while(tr_iter.Valid()) {
      uint64_t *key = (uint64_t *)tr_iter.Key();
      if(!compareSecurityKey((uint64_t)key,(uint64_t)(ticker.symbol))) break;
      //	  MemNode *node = (MemNode *)(tr_iter.Node());
      //	  uint64_t seq = node->seq;
      //	  tr_iter.Next();
      //	  continue;
      trade_request::value *tr;

      uint64_t seq = tx_->get(TRADE_REQ,tr_iter.Key(),(char **)(&tr),sizeof(trade_request::value));
      if(unlikely(seq == 0)) {
        tr_iter.Next();
        continue;
      }

      tx_->delete_by_node(TRADE_REQ,tr_iter.Node());
      uint64_t sec_key = transferToTRSec(tr_iter.Key());
      tx_->delete_index(SEC_SC_TR,sec_key);

      /* fetch trade */
      uint64_t trade_id = key[3];
      int pid = decode_trade_mac(trade_id);
      if(pid == current_partition) {
        local_trades.push_back(trade_id);
      }
      else {
        tx_->add_to_remote_set(TRADE,trade_id,pid);
        uint64_t th_key = makeTHKey(trade_id,now_dts,"SBMT");
        tx_->remote_insert(TRADE_HIST,(uint64_t *)th_key,3 * sizeof(uint64_t),pid);
        num_remote_trades += 1;
      }
      tr_len += 1;
      tr_iter.Next();
    }

    // FIXME!! may modify indexes
    int num_servers(0);
    if(num_remote_trades > 0) {
      assert(tx_->cor_id_ == cor_id_);
      num_servers = tx_->do_remote_reads();
    }
    for(uint i = 0; (i < local_trades.size());++i) {
#if 1
      trade::value *tv;
      uint64_t seq = tx_->get(TRADE,local_trades[i],(char **)(&tv),sizeof(trade::value));

      if(unlikely(seq == 1)) continue;
      //	  if(tv->t_dts > now_dts) {
      //	    fprintf(stdout,"dts check failed %lu %lu @%d,%d\n",now_dts,tv->t_dts,worker_id_,cor_id_);
      //	    assert(false);
      //	  }
      //	  tv->t_dts = now_dts;
      if(unlikely(now_dts < tv->t_dts)) { }
      else
        tv->t_dts = now_dts;
      tv->t_st_id = std::string("SBMT");
      tx_->write();
#endif
      uint64_t th_key = makeTHKey(local_trades[i],now_dts,"SBMT");
      char *dummy = new char[meta_size + sizeof(bool)];
      tx_->insert(TRADE_HIST,th_key,dummy,sizeof(trade_history::value));
    }
    if(num_remote_trades > 0) {

      indirect_yield(yield);
      tx_->get_remote_results(num_servers);

      for(uint i = 0;i < num_remote_trades;++i) {
        trade::value *tv;
        uint64_t seq = tx_->get_cached(i * 2,(char **)(&tv));
        //assert(seq != 1);
#ifdef SI_TX
        if(unlikely(seq == 1))
          continue;
#else
        assert(seq != 1);
#endif
        tv->t_st_id = std::string("SBMT");
        if(unlikely(tv->t_dts >= now_dts)) {
          /* clock skew, just pass */
        }
        else
          tv->t_dts   = now_dts;
        bool dummy;
        tx_->remote_write(i * 2,(char *)tv,sizeof(trade::value));
        tx_->remote_write(i * 2 + 1,(char *)(&dummy),sizeof(bool));
      }
    }
#endif
    bool ret = tx_->end(yield);
    if(!ret) {
      //	  assert(false);
      return txn_result_t(false,0);
    } else {
      //fprintf(stdout,"market feed size %d, %d\n",tx_->report_rw_set(),tx_->remoteset->read_items_);
      //sleep(1);
#if PROFILE_SERVER_NUM == 1
      rwsize += 1 + tx_->remoteset->get_broadcast_num();
#else
      rwsize += (tx_->report_rw_set() + tx_->remoteset->read_items_ + tx_->remoteset->write_items_);
#endif
      processed_count += 1;
    }
#endif
    market_feed_process_idx[cor_id_] += 1;
  }
  market_feed_process_idx[cor_id_] = -1;
  // FIXME, for now we just omit SendToMarketFromFrame
  return txn_result_t(true,(double)rwsize);
}

txn_result_t TpceWorker::txn_trade_update(yield_func_t &yield) {

  TTradeUpdateTxnInput input;
  input_generator_->SetRNGSeed(random_generator[cor_id_].get_seed());
  input_generator_->GenerateTradeUpdateInput(input);

  if(input.frame_to_execute == 1) {
    return trade_update_frame1(yield,input);
  } else if(input.frame_to_execute == 2) {
    return trade_update_frame2(yield,input);
  } else if(input.frame_to_execute == 3) {
    return trade_update_frame3(yield,input);
  }
  assert(false);
  return txn_result_t(true,73);
}

txn_result_t TpceWorker::trade_update_frame1(yield_func_t &yield,TTradeUpdateTxnInput &input) {

  tx_->begin(db_logger_);
  int num_updates(0);

  const char *min_s = "\0";
  for(uint i = 0;i < input.max_trades;++i) {
    uint64_t tid = encode_trade_id(input.trade_id[i],0,0);
    trade::value *tv;
    uint64_t seq = tx_->get(TRADE,tid,(char **)(&tv),sizeof(trade::value));
    assert(seq != 1);
    //	fprintf(stdout,"get tid %lu\n",tid);
    trade_type::value *tt = TpceTradeHash[tv->t_tt_id.data()];

    if(num_updates < input.max_updates) {
      num_updates ++;
      tx_->write();

      std::string temp_exec_name = tv->t_exec_name.data();
      //	  fprintf(stdout,"test str %s\n",temp_exec_name.c_str());
      size_t index = temp_exec_name.find(" X ");
      if(index != std::string::npos){
        temp_exec_name.replace(index, 3, "   ");
      } else {
        index = temp_exec_name.find(" ");
        //	    fprintf(stdout,"str idx %d\n",index);
        //	    assert(index != std::string::npos);
        temp_exec_name.replace(index, 3, " X ");
      }
      tv->t_exec_name = temp_exec_name;

    }

    /* fetching some data */
    /* retrive trade */
    settlement::value *set;
    seq = tx_->get(SETTLEMENT,tid,(char **)(&set),sizeof(settlement::value));
    assert(seq != 1);

    if(tv->t_is_cash) {
      cash_transaction::value *ct;
      seq = tx_->get(CASH_TX,tid,(char **)(&ct),sizeof(cash_transaction::value));
#if 0
      if(seq == 1) {
        fprintf(stdout,"get tid %lu is cash %d\n",tid,trade.t_is_cash);
        fprintf(stdout,"sanity check trade comm %f name %s\n",trade.t_comm,
                trade.t_exec_name.data());
        assert(false);
      }
#else
      assert(seq != 1);
#endif
    }
    uint64_t th_start_key = makeTHKey(tid,0,min_s);

#ifdef RAD_TX
    RadIterator th_iter((DBRad *)tx_,TRADE_HIST,false);
#elif defined(OCC_TX)
    DBTXIterator th_iter((DBTX *)tx_,TRADE_HIST,false);
#elif defined(FARM)
    DBFarmIterator th_iter((DBFarm *)tx_,TRADE_HIST,false);
#elif defined(SI_TX)
    SIIterator th_iter((DBSI *)tx_,TRADE_HIST,false);
#else
    assert(false);
#endif
    int th_cursor(0);
    th_iter.Seek(th_start_key);
    while(th_iter.Valid()) {
      uint64_t *key = (uint64_t *)th_iter.Key();
      if(key[0] != tid || th_cursor >= TradeLookupMaxTradeHistoryRowsReturned) break;
      trade_history::value *th;
      seq = tx_->get(TRADE_HIST,th_iter.Key(),(char **)(&th),sizeof(trade_history::value));
      assert(seq != 1);
      th_cursor += 1;

      th_iter.Next();
    }

  }
  bool ret = tx_->end(yield);
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret, tx_->remoteset->get_broadcast_num() + 1);
#else
  return txn_result_t(ret,tx_->report_rw_set() + tx_->remoteset->read_items_ + tx_->remoteset->write_items_);
#endif
}

txn_result_t TpceWorker::trade_update_frame2(yield_func_t &yield,TPCE::TTradeUpdateTxnInput &input) {

  tx_->begin(db_logger_);

  auto start_dts = CDateTime((TIMESTAMP_STRUCT*)&input.start_trade_dts).GetDate();
  auto end_dts = CDateTime((TIMESTAMP_STRUCT*)&input.end_trade_dts).GetDate();

  uint64_t t_ca_start_key =
      makeSecondCATrade(input.acct_id,
                        start_dts,
                        0);

  assert( caToPartition(input.acct_id) == current_partition);

  //      std::vector<trade::value *> found_trades; found_trades.reserve(input.max_trades);
  //      std::vector<uint64_t> tids;tids.reserve(input.max_trades);
  const char *min_s = "\0";

#ifdef RAD_TX
  RadIterator t_ca_iter((DBRad *)tx_,SEC_CA_TRADE,true);
#elif defined(OCC_TX)
  DBTXIterator t_ca_iter((DBTX *)tx_,SEC_CA_TRADE,true);
#elif defined(FARM)
  DBFarmIterator t_ca_iter((DBFarm *)tx_,SEC_CA_TRADE,true);
#elif defined(SI_TX)
  SIIterator t_ca_iter((DBSI *)tx_,SEC_CA_TRADE,true);
#else
  assert(false);
#endif
  t_ca_iter.Seek(t_ca_start_key);
  int num_found(0), num_updates(0);

  while(t_ca_iter.Valid()) {

    uint64_t *key = (uint64_t *)t_ca_iter.Key();
    if(num_found > input.max_trades || key[0] != input.acct_id || key[1] > end_dts) break;
    trade::value *tv;
    uint64_t seq = tx_->get(TRADE,key[2],(char **)(&tv),sizeof(trade::value));
    assert(seq != 1);

    settlement::value *sv;
    seq = tx_->get(SETTLEMENT,key[2],(char **)(&sv),sizeof(settlement::value));
    if(num_updates < input.max_updates) {
      if(tv->t_is_cash) {
        if(sv->se_cash_type == "Cash Account") sv->se_cash_type = "Cash";
        else sv->se_cash_type = "Cash Account";
      } else {
        if(sv->se_cash_type == "Margin Account") sv->se_cash_type = "Margin";
        else sv->se_cash_type = "Margin Account";
      }
      num_updates += 1;
      tx_->write();
    }
    if(tv->t_is_cash) {
      cash_transaction::value *ct;
      seq = tx_->get(CASH_TX,key[2],(char **)(&ct),sizeof(cash_transaction::value));
      assert(seq != 1);
    }

    uint64_t th_start_key = makeTHKey(key[2],0,min_s);

#ifdef RAD_TX
    RadIterator th_iter((DBRad *)tx_,TRADE_HIST,false);
#elif defined(OCC_TX)
    DBTXIterator th_iter((DBTX *)tx_,TRADE_HIST,false);
#elif defined(FARM)
    DBFarmIterator th_iter((DBFarm *)tx_,TRADE_HIST,false);
#elif defined(SI_TX)
    SIIterator th_iter((DBSI *)tx_,TRADE_HIST,false);
#else
    assert(false);
#endif
    int th_cursor(0);
    th_iter.Seek(th_start_key);
    while(th_iter.Valid()) {
      uint64_t *key_h = (uint64_t *)th_iter.Key();
      if(key_h[0] != key[2]
         || th_cursor >= TradeUpdateMaxTradeHistoryRowsReturned) break;
      trade_history::value *th;
      seq = tx_->get(TRADE_HIST,th_iter.Key(),(char **)(&th),sizeof(trade_history::value));
      assert(seq != 1);
      th_cursor += 1;
      th_iter.Next();
    }

    num_found += 1;
    t_ca_iter.Next();
  }

  bool ret = tx_->end(yield);
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret, tx_->remoteset->get_broadcast_num() + 1);
#else
  return txn_result_t(ret,tx_->report_rw_set() + tx_->remoteset->read_items_ + tx_->remoteset->write_items_);
#endif
}


txn_result_t TpceWorker::trade_update_frame3(yield_func_t &yield,TTradeUpdateTxnInput &input) {
  //  sleep(1);
#if 0
  // XD: I've checked that each server receive the amount of security requets
  uint64_t co_id = SecurityToCompany[input.symbol];
  mac_heatmap[companyToPartition(co_id)] += 1;
#endif
  tx_->begin(db_logger_);
  uint64_t co_id = SecurityToCompany[input.symbol];
  assert(companyToPartition(co_id) == current_partition);

  uint64_t start_dts = CDateTime((TIMESTAMP_STRUCT*)&input.start_trade_dts).GetDate();
  uint64_t end_dts = CDateTime((TIMESTAMP_STRUCT*)&input.end_trade_dts).GetDate();

  uint64_t sec_s_key = makeSecondSecTrade(input.symbol,start_dts,0);
  //          char *test_symbol = "TTES";
  //          uint64_t sec_s_key = makeSecondSecTrade(input.symbol,0,0);
  //        fprintf(stdout,"test symbol %s %lu %lu\n",sec_s_key,start_dts,end_dts);
  //          uint64_t sec_s_key = makeSecondSecTrade(test_symbol,0,0);
#ifdef RAD_TX
  RadIterator sec_iter((DBRad *)tx_,SEC_S_T,true);
#elif defined(OCC_TX)
  DBTXIterator sec_iter((DBTX *)tx_,SEC_S_T,true);
#elif defined(FARM)
  DBFarmIterator sec_iter((DBFarm *)tx_,SEC_S_T,true);
#elif defined(SI_TX)
  SIIterator sec_iter((DBSI *)tx_,SEC_S_T,true);
#else
  assert(false);
#endif
  sec_iter.Seek(sec_s_key);
  int num_found(0);
  // In default setting all trade updates rows must be updated
  //      int local_update_num(0), remote_update_num(0),total_update_num(0);

  int num_remote_trades(0); std::vector<uint64_t> local_trades;
  while(sec_iter.Valid()) {
    uint64_t *key = (uint64_t *)sec_iter.Key();
    if(num_found >= input.max_trades ||
       !compareSecurityKey((uint64_t ) (&key[0]), (uint64_t)(input.symbol))
       || key[3] > end_dts)
      break;
    uint64_t tid = key[4];

    int pid;
    if( (pid = decode_trade_mac(tid)) != current_partition) {
      tx_->add_to_remote_set(TRADE,tid,pid);
      tx_->add_to_remote_set(CASH_TX,tid,pid);
      tx_->add_to_remote_set(SETTLEMENT,tid,pid);
      // FIXME !! currently not supported ranged search
      // maybe uses a forkhandler to do so, since conflicts are tracked by trade object
      num_remote_trades += 1;
    } else {
      //	  fprintf(stdout,"found local trades %lu\n",tid);
      local_trades.push_back(tid);
    }
    num_found += 1;
    sec_iter.Next();
  }
   
  int num_servers(0);
  if(num_remote_trades > 0) {
    //num_servers = tx_->remoteset->do_reads(3);
    num_servers = tx_->do_remote_reads();
  }

  for(uint i = 0;i < local_trades.size();++i) {

    trade::value *tv;
    uint64_t seq = tx_->get(TRADE,local_trades[i],(char **)(&tv),sizeof(trade::value));
    assert(seq != 1);

    trade_type::value *tt = TpceTradeHash[tv->t_tt_id.data()];
    uint64_t sec_key = makeSecurityIndex(tv->t_s_symb.data());
    security::value *sv;
    seq = tx_->get(SECURITY,sec_key,(char **)(&sv),sizeof(security::value));
    assert(seq != 1);

    if(tv->t_is_cash) {
      cash_transaction::value *cv;
      uint64_t seq = tx_->get(CASH_TX,local_trades[i],(char **)(&cv),sizeof(cash_transaction::value));
      assert(seq != 1);

      std::string temp_ct_name = cv->ct_name.str();
      size_t index = temp_ct_name.find(" shares of ");

      if(index != std::string::npos){
        stringstream ss;
        ss << tt->tt_name.data() <<  " " << tv->t_qty << " shares of " <<
            " Shares of " << sv->s_name.data();
        cv->ct_name = ss.str();
      } else {
        stringstream ss;
        ss << tt->tt_name.data() <<  " " << tv->t_qty << " shares of "
           << " shares of " << sv->s_name.data();
        cv->ct_name = ss.str();
      }
      tx_->write();
    }
    settlement::value *stv;
    seq = tx_->get(SETTLEMENT,local_trades[i],(char **)(&stv),sizeof(settlement::value));
    assert(seq != 1);

    /* TODO!!, fetching trade history? */
  }

  if(num_remote_trades > 0) {
    // yield(routines_[0]);
    indirect_yield(yield);
    tx_->get_remote_results(num_servers);

    for(uint i = 0;i < num_remote_trades;++i) {

      trade::value *tv;
      uint64_t seq = tx_->get_cached(i * 3,(char **)(&tv));
#ifdef SI_TX
      if(unlikely(seq == 1)) continue;
#else
      assert(seq != 1);
#endif
      if(tv->t_is_cash) {

        trade_type::value *tt = TpceTradeHash[tv->t_tt_id.data()];
        uint64_t sec_key = makeSecurityIndex(tv->t_s_symb.data());
        security::value *sv;
        uint64_t seq = tx_->get(SECURITY,sec_key,(char **)(&sv),sizeof(security::value));
        assert(seq != 1);

        cash_transaction::value *ctv;
        seq = tx_->get_cached(i * 3 + 1, (char **)(&ctv));
#ifdef SI_TX
        if(unlikely(seq == 1)) continue;
#else
        assert(seq != 1);
#endif
        std::string temp_ct_name = ctv->ct_name.str();
        size_t index = temp_ct_name.find(" shares of ");

        if(index != std::string::npos){
          stringstream ss;
          ss << tt->tt_name.data() <<  " " << tv->t_qty << " shares of "
             << " Shares of " << sv->s_name.data();
          ctv->ct_name = ss.str();
        } else {
          stringstream ss;
          ss << tt->tt_name.data() <<  " " << tv->t_qty << " shares of "
             << " shares of " << sv->s_name.data();
          ctv->ct_name = ss.str();
        }

        /* update */
        tx_->remote_write(i * 3 + 1, (char *)ctv,sizeof(cash_transaction::value));

        // FIXME!! , fetching trade history?
      }
    }
  }

  bool ret = tx_->end(yield);
#if PROFILE_SERVER_NUM == 1
  return txn_result_t(ret, tx_->remoteset->get_broadcast_num() + 1);
#else
  return txn_result_t(ret,tx_->report_rw_set() + tx_->remoteset->read_items_ + tx_->remoteset->write_items_);
#endif
}
/* end namespace tpce */
};
};
};
