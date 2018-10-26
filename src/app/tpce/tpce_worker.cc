#include "tx_config.h"

#include "tpce_worker.h"

#include "util/mapped_log.h"

#include "egen/EGenLoader_stdafx.h"
#include "egen/EGenGenerateAndLoad.h"

//#include "framework/rpc.h"
#include "framework/req_buf_allocator.h"
#include "core/logging.h"

#include <boost/bind.hpp>
#include <queue>

extern size_t total_partition;
extern size_t current_partition;

extern __thread MappedLog local_log;

using namespace TPCE;

namespace nocc {

using namespace rtx;

// db utilities
extern __thread oltp::RPCMemAllocator *msg_buf_alloctors;

static SpinLock exit_lock;
extern RdmaCtrl *cm;

namespace oltp {

extern __thread util::fast_random   *random_generator;

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
  //fprintf(stdout,"init trade_id %lu, required %lld\n",trade_id_,
  //input_generator_->m_iMaxActivePrePopulatedTradeID);
  assert(lastTradeId < std::numeric_limits<uint32_t>::max());
}

void TpceWorker::register_callbacks() {

  fprintf(stdout,"TPCE register callbacks\n");
  //rpc_->register_callback(bind(&TpceWorker::add_market_feed,this,_1,_2,_3,_4), \
                          //RPC_ADD);
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

#if 1 // a special hook to test TPCE's read-only TX
  w.push_back(workload_desc("Customer position",1,TxnCustomerPos));
  return w;
#endif

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

#if ONE_SIDED_READ
    new_txs_[i] = new OCCR(this,store_,rpc_,current_partition,worker_id_,i,current_partition,
                           cm,rdma_sched_,total_partition);
#else
    new_txs_[i] = new OCC(this,store_,rpc_,current_partition,i,current_partition);
#endif
  }
  /* init local tx so that it is not a null value */
  rtx_hook_ = new_txs_[1];
}
}; // namespace tpce
};
};
