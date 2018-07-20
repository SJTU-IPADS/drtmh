#include "tpce_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"

extern size_t current_partition;
extern __thread RemoteHelper *remote_helper;

namespace nocc {
namespace oltp {
namespace tpce {

extern     std::map<std::string,uint64_t>    SecurityToCompany;
extern     std::map<std::string,std::string> TpceSector;

extern     __thread std::queue<TPCE::TTickerEntry>         *marketFeedQueue;

void TpceWorker::customer_position_piece(yield_func_t &yield,int id, int cid,char *input) {

  CPInputHeader *header = (CPInputHeader *)input;
  CPInputItem   *p = (CPInputItem *)(input + sizeof(CPInputHeader));
  //fprintf(stdout,"recv time %lu num %d\n",header->time,header->secs);
#ifdef OCC_TX
  RemoteHelper *h =  remote_helper;
  h->begin(_QP_ENCODE_ID(id,cid + 1));
  //tx_ = h->temp_tx_;
#endif

#ifdef SI_TX
  uint64_t timestamp = (uint64_t)(&(header->ts_vec));
#else
  uint64_t timestamp = header->time;
#endif

  char reply_tmp_buf[MAX_MSG_SIZE];
  CPOutput *reply = (CPOutput *)reply_tmp_buf;

  int reply_count  = 0;
  assert(header->secs <= 256);
  for(uint i = 0;i < header->secs;++i) {
    if(p[i].pid != current_partition) continue;
    reply[reply_count].idx0 = p[i].idx0;
    reply[reply_count].idx1 = p[i].idx1;
    /* some sanity checks */
    assert(p[i].idx0 < 16);
    last_trade::value lt;

    if(unlikely(tx_->get_ro_versioned(LT,(uint64_t )(&(p[i].name)),
                                      (char *)(&lt),timestamp,yield) == 0)) {
      assert(false);
    }
    reply[reply_count].val = lt.lt_price;
    //	if(lt.lt_price == 0) {
    //	  fprintf(stdout,"last trade %s\n",p[i].name);
    //	  assert(false);
    //	}
    assert(lt.lt_price > 0);
    reply_count += 1;
    //	fprintf(stdout,"send reply %d %d || %f\n",p[i].idx0,p[i].idx1,lt.lt_price);
  }
  assert(reply_count > 0);
  //      assert(false);
  CPOutput *reply_msg  = (CPOutput *)(rpc_->get_reply_buf());
  if(unlikely(sizeof(CPOutput) * reply_count + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(rpc_header)
              > MAX_MSG_SIZE)) {
    fprintf(stdout,"customer reply boom, count %d, input num %d\n",reply_count,header->secs);
    assert(false);
  }
  memcpy(reply_msg,reply_tmp_buf,sizeof(CPOutput) * reply_count);
  rpc_->send_reply((char *)reply_msg,reply_count * sizeof(CPOutput), id, worker_id_,cid);
}

void TpceWorker::security_detail_piece(yield_func_t &yield,int id, int cid,char *input) {

#ifdef OCC_TX
  RemoteHelper *h =  remote_helper;
  h->begin(_QP_ENCODE_ID(id,cid + 1));
  //tx_ = h->temp_tx_;
#endif

#if 1
  SEDInput *s = (SEDInput *)input;

#ifdef SI_TX
  uint64_t timestamp = (uint64_t)(&(s->ts_vec));
  assert(s->ts_vec[0] != 0);
#else
  uint64_t timestamp = s->time;
#endif

  SEDOutput reply;
  last_trade::value lt;
  auto key = makeSecurityIndex(s->name);
  uint64_t seq = tx_->get_ro_versioned(LT,key,(char *)(&lt),
                                       timestamp,yield);
  delete (char *)key;
  assert(seq != 1);
  reply.lt_price = lt.lt_price;
  reply.lt_open_price = lt.lt_open_price;
  reply.lt_vol = lt.lt_vol;
#endif
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(struct SEDOutput),id,worker_id_,cid);
}

void TpceWorker::trade_lookup_piece0(yield_func_t &yield,int id, int cid,char *input) {

#ifdef OCC_TX
  RemoteHelper *h = remote_helper;
  h->begin(_QP_ENCODE_ID(id,cid + 1));
  //tx_ = h->temp_tx_;
#endif

  TradeLookupInput *in = (TradeLookupInput *)input;


#ifdef SI_TX
  uint64_t timestamp = (uint64_t)(&(in->ts_vec));
#else
  uint64_t timestamp = in->time;
#endif
  assert(companyToPartition(SecurityToCompany[in->name]) == current_partition);

  TradeLookupOutput1 out;

  uint64_t sec_s_key = makeSecondSecTrade(in->name,in->start_dts,0);
#ifdef RAD_TX
  RadIterator sec_iter((DBRad *)tx_,SEC_S_T,true);
#elif  defined(OCC_TX)
  DBTXIterator sec_iter(h->temp_tx_,SEC_S_T,true);
#elif  defined(FARM)
  DBFarmIterator sec_iter((DBFarm *)tx_,SEC_S_T,true);
#elif  defined(SI_TX)
  SIIterator sec_iter((DBSI *)tx_,SEC_S_T,true);
#else
  /* not implemented yet */
  assert(false);
#endif

  sec_iter.Seek(sec_s_key);
  int num_found(0);
  while(sec_iter.Valid()) {
    uint64_t *key = (uint64_t *)sec_iter.Key();
    if(num_found >= TPCE::TradeLookupMaxRows ||
       !compareSecurityKey((uint64_t ) (&key[0]), (uint64_t)(in->name))
       || key[3] > in->end_dts)
      break;

    out.trade_ids[num_found++] = key[4];
    sec_iter.Next();
  }
  assert(num_found <= 256);
  out.count = num_found;
  CPOutput *reply_msg  = (CPOutput *)(rpc_->get_reply_buf());
  memcpy(reply_msg, (char *)(&out),sizeof(TradeLookupOutput1));
  rpc_->send_reply((char *)reply_msg,sizeof(TradeLookupOutput1), id,worker_id_,cid);
}

void TpceWorker::trade_lookup_piece1(yield_func_t &yield,int id,int cid, char *input) {

#ifdef OCC_TX
  RemoteHelper *h =  remote_helper;
  h->begin(_QP_ENCODE_ID(id,cid + 1));
  //tx_ = h->temp_tx_;
#endif

  TradeLookupOutput1 *in = (TradeLookupOutput1 *)input;
#ifdef SI_TX
  uint64_t timestamp = (uint64_t)(&(in->ts_vec));
#else
  uint64_t timestamp = in->time;
#endif

  uint64_t dummy_reply;
  const char *min_s = "\0";
  for(uint i = 0;i < in->count;++i) {
    uint64_t tid = in->trade_ids[i];
    if(decode_trade_mac(tid) == current_partition) {

      settlement::value set;
      uint64_t seq = tx_->get_ro_versioned(SETTLEMENT,tid,(char *)(&set),timestamp,yield);
      assert(seq != 1);
      trade::value tv;
      seq = tx_->get_ro_versioned(TRADE,tid,(char *)(&tv),timestamp,yield);
      assert(seq != 1);
      if(tv.t_is_cash) {
        cash_transaction::value ct;
        seq = tx_->get_ro_versioned(CASH_TX,tid,(char *)(&ct),timestamp,yield);
        assert(seq != 1);
      }
      uint64_t th_start_key = makeTHKey(tid,0,min_s);
#ifdef RAD_TX
      RadIterator th_iter((DBRad *)tx_,TRADE_HIST,false);
#elif defined(OCC_TX)
      DBTXIterator th_iter(h->temp_tx_,TRADE_HIST,false);
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
           || th_cursor >= TPCE::TradeLookupMaxTradeHistoryRowsReturned) break;
        trade_history::value th;
        seq = tx_->get_ro_versioned(TRADE_HIST,th_iter.Key(),(char *)(&th),timestamp,yield);
        assert(seq != 1);

        th_cursor += 1;
        th_iter.Next();
      }
    } else continue;
    /* end iterating trades */
  }
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(uint64_t),id,worker_id_,cid);
}

void TpceWorker::broker_volumn_piece(yield_func_t &yield,int id, int cid,char *input) {

  BrokerVolumnInput *arg = (BrokerVolumnInput *)input;
#ifdef OCC_TX
  RemoteHelper *h =  remote_helper;
  h->begin(_QP_ENCODE_ID(id,cid + 1));
  //tx_ = h->temp_tx_;
#endif

#if 0  // no execute logic
#ifdef SI_TX
  uint64_t timestamp = (uint64_t)(&(arg->ts_vec));
#else
  uint64_t timestamp = arg->time;
#endif

  auto &sc_id = TpceSector[arg->input.sector_name];
  const char *min_s = "\0";

  double   broker_volumn(0);
  //fprintf(stdout,"num %d\n",arg->input.broker_num);
  for(uint i = 0;i < arg->input.broker_num;++i) {
    if(brokerToPartition(arg->input.broker_list[i]) != current_partition) {
      continue;
    }
#if 0
    broker::value bv;
    uint64_t seq = tx_->get_ro_versioned(BROKER,arg->input.broker_list[i],(char *)(&bv),timestamp,yield);
    assert(seq != 1);
#endif
    int row_counts(0);
    uint64_t sec_s_key = makeSecondTradeRequest(sc_id.data(),arg->input.broker_list[i],
                                                min_s,0);

#ifdef RAD_TX
    RadIterator tr_iter((DBRad *)tx_,SEC_SC_TR,true);
#elif defined(OCC_TX)
    DBTXIterator tr_iter(h->temp_tx_,SEC_SC_TR,true);
#elif defined(FARM)
    DBFarmIterator tr_iter((DBFarm *)tx_,SEC_SC_TR,true);
#elif defined(SI_TX)
    SIIterator tr_iter((DBSI *)tx_,SEC_SC_TR,true);
#else
#endif

    tr_iter.Seek(sec_s_key);
    while(tr_iter.Valid()) {
      uint64_t *key = (uint64_t *)(tr_iter.Key());
      if(key[1] != arg->input.broker_list[i]) break;
      uint64_t k = transferToTR(tr_iter.Key());
      trade_request::value v;
      uint64_t seq = tx_->get_ro_versioned(TRADE_REQ,k,(char *)(&v),timestamp,yield);
      if(unlikely(seq == 1)) { tr_iter.Next();continue;}

      broker_volumn += v.tr_bid_price * v.tr_qty;
      row_counts += 1;
      tr_iter.Next();
    }
  }
#endif  // end no execute logic
  char *reply = rpc_->get_reply_buf();
  rpc_->send_reply(reply,sizeof(uint64_t),id,worker_id_,cid);
}

void TpceWorker::market_watch_piece(yield_func_t &yield,int id,int cid, char *input) {
  MarketWatchInput *header = (MarketWatchInput *)input;
  MWInputItem *p = (MWInputItem *)(input + sizeof(MarketWatchInput));

#ifdef OCC_TX
  RemoteHelper *h = remote_helper;
  h->begin( _QP_ENCODE_ID(id,cid + 1));
  //tx_ = h->temp_tx_;
#endif

#ifdef SI_TX
  uint64_t timestamp = (uint64_t)(&(header->ts_vec));
#else
  uint64_t timestamp = header->time;
#endif

#if 1
  for(uint i = 0;i < header->secs;++i) {
    if(p[i].pid  != current_partition) continue;

    last_trade::value ltv;
    uint64_t key = (uint64_t)(p[i].name);
    uint64_t seq = tx_->get_ro_versioned(LT,key,(char *)(&ltv),timestamp,yield);
    assert(seq != 1);

    security::value sec;
    seq = tx_->get_ro_versioned(SECURITY,key,(char *)(&sec),timestamp,yield);
    assert(seq != 1);
    uint64_t dm_key = makeDMKey(p[i].name,header->start_day);

    daily_market::value dm;
    seq = tx_->get_ro_versioned(DAILY_MARKET,dm_key,(char *)(&dm),timestamp,yield);
    assert(seq != 1);
  }
#endif
  char *reply_msg = rpc_->get_reply_buf();
  rpc_->send_reply(reply_msg,sizeof(uint64_t),id,worker_id_,cid); /* send a dummy */
  //this->context_transfer();
}

/***** helper rpcs ********/

void TpceWorker::add_market_feed(int id,int cid, char *input, void *arg) {
  TPCE::TTickerEntry *entry = (TPCE::TTickerEntry *)input;
  marketFeedQueue->push(*entry);
  return ;
}
};
};
};
