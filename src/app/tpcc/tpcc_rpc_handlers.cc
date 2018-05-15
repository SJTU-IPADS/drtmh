#include "tpcc_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"

extern __thread RemoteHelper *remote_helper;
extern size_t current_partition;

namespace nocc {
  namespace oltp {
  namespace tpcc {

    void TpccWorker::stock_level_piece(yield_func_t &yield,int id,int cid,char *input) {

      StockLevelInput  *header = (StockLevelInput *)input;
      StockLevelInputPayload *p = (StockLevelInputPayload *)(input + sizeof(StockLevelInput));
      int threshold = header->threshold;

#ifdef SI_TX
      uint64_t timestamp = (uint64_t )(&(header->ts_vec));
#else
      uint64_t timestamp = header->timestamp;
#endif
      assert(header->num > 0);
#ifdef OCC_TX
      RemoteHelper *h =  remote_helper;
      h->begin(_QP_ENCODE_ID(id,cid + 1));
#endif

      int res = 0;
      /* parse the input */
      for(uint i = 0;i < header->num;++i) {
        if(p[i].pid != current_partition)  {
          continue;
        }
        /* execute piece */
        //fprintf(stdout,"fetch %d %d %p\n",p[i].warehouse_id,p[i].district_id,tx_);
        uint64_t d_key = makeDistrictKey(p[i].warehouse_id,p[i].district_id);
        district::value v_d;
        uint64_t d_seq = tx_->get_ro_versioned(DIST,d_key,(char *)(&v_d),timestamp,yield);
        //goto END;
        assert(d_seq != 0);
        uint64_t cur_next_o_id = v_d.d_next_o_id;

        const int32_t lower = cur_next_o_id >= STOCK_LEVEL_ORDER_COUNT ? (cur_next_o_id - STOCK_LEVEL_ORDER_COUNT) : 0;
        uint64_t start = makeOrderLineKey(p[i].warehouse_id, p[i].district_id, lower, 0);
        uint64_t end   = makeOrderLineKey(p[i].warehouse_id, p[i].district_id, cur_next_o_id, 0);

#ifdef RAD_TX
        RadIterator iter((DBRad *)tx_,ORLI);
#elif defined(OCC_TX)
        DBTXTempIterator iter((DBTX *)tx_,ORLI);
#elif defined(FARM)
        DBFarmIterator iter((DBFarm *)tx_,ORLI);
#elif defined(SI_TX)
        SIIterator iter((DBSI *)tx_,ORLI);
#endif
        iter.Seek(start);

        while(iter.Valid()) {
          int64_t ol_key = iter.Key();
          if(ol_key >= end) break;
          order_line::value v_ol;
          if(unlikely(tx_->get_ro_versioned(ORLI,ol_key,(char *)(&v_ol),timestamp,yield) == 0)) {
            fprintf(stdout,"d_key seq %lu\n",d_seq);
            assert(false);
            goto NEXT;
          }
          {
            int64_t s_key = makeStockKey(p[i].warehouse_id, v_ol.ol_i_id);
            stock::value v_s;
            tx_->get_ro_versioned(STOC,s_key,(char *)(&v_s),timestamp,yield);
            if(v_s.s_quantity < int(threshold)) {
              res += 1;
            }
          }
        NEXT:
          iter.Next();
        }
        /* end for iterating requests */
      }
    END:
      /* reply */
      char *reply_msg = rpc_->get_reply_buf();
      ((StockLevelReply *)reply_msg)->num_items = res;
      rpc_->send_reply(reply_msg,sizeof(StockLevelReply),id,worker_id_,cid);

      this->context_transfer();
    }

  };
};
};
