#include "global_config.h"
#include "tpce_schema.h"
#include "tpce_worker.h"
#include "memstore/memdb.h"

// for parsing config xml
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>


/* leverages this for TPCE file loading */
#include "egen/EGenLoader_stdafx.h"
#include "egen/EGenGenerateAndLoad.h"

/* TPCE constants */
#include "egen/MiscConsts.h"
#include "egen/DM.h"


#include "db/txs/dbsi.h"

#include <iostream>

using namespace std;
using namespace TPCE;

extern int verbose;
extern size_t nclients;
extern size_t nthreads;
extern size_t current_partition;
extern size_t total_partition;

namespace nocc {
  namespace oltp {
    namespace tpce {

      /* parameters */
      int totalCustomer;
      int scaleFactor;
      int mStartCustomer;
      int tradeDays;

      /* cp,to,tr,sd,tl,ts,bv,mf,mw,tu */
      unsigned g_txn_workload_mix[10] = { 13,10,10,14 ,8,19,5,1,18,2 };
      //unsigned g_txn_workload_mix[10] = {   0, 0, 0, 100,0,0,0,0,0,0 };

      /* The logger and the input files are shared */
      CInputFiles *inputFiles = NULL;
      CEGenLogger *logger = NULL;

      class TpceClient : public BenchClient {
      public:
        TpceClient(unsigned worker_id,unsigned seed)
          : BenchClient(worker_id,seed) {}
        virtual int get_workload(char *input,util::fast_random &rand) {
          // pick execute machine
          uint pid  = rand.next() % total_partition;

          // pick tx idx
          static auto workload = TpceWorker::_get_workload();
          double d = rand.next_uniform();

          uint tx_idx = 0;
          for(size_t i = 0;i < workload.size();++i) {
            if((i + 1) == workload.size() || d < workload[i].frequency) {
              tx_idx = i;
              break;
            }
            d -= workload[i].frequency;
          }
          *(uint8_t *)input = tx_idx;
          return pid;
        }
      };


      void parse_workload(std::string &config_file) {
        using boost::property_tree::ptree;
        using namespace boost;
        using namespace property_tree;
        try {
          // parse each TX's ratio
          ptree pt;
          read_xml(config_file,pt);

          int cp = pt.get<int>("bench.tpce.cp");
          g_txn_workload_mix[0] = cp;
          int to = pt.get<int>("bench.tpce.to");
          g_txn_workload_mix[1] = to;
          int tr = pt.get<int>("bench.tpce.tr");
          g_txn_workload_mix[2] = tr;
          int sd = pt.get<int>("bench.tpce.sd");
          g_txn_workload_mix[3] = sd;
          int tl = pt.get<int>("bench.tpce.tl");
          g_txn_workload_mix[4] = tl;
          int ts = pt.get<int>("bench.tpce.ts");
          g_txn_workload_mix[5] = ts;
          int bv = pt.get<int>("bench.tpce.bv");
          g_txn_workload_mix[6] = bv;
          int mf = pt.get<int>("bench.tpce.mf");
          g_txn_workload_mix[7] = mf;
          int mw = pt.get<int>("bench.tpce.mw");
          g_txn_workload_mix[8] = mw;
          int tu = pt.get<int>("bench.tpce.tu");
          g_txn_workload_mix[9] = tu;
          
        } catch (const ptree_error &e) {
          fprintf(stdout,"[TPCE] using default workload mix\n");
          // pass
        }

      }

      class TpceMainRunner : public BenchRunner {
      public:
        TpceMainRunner(std::string &config_file) ;
        virtual void init_put();
        virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB* store = NULL);
        virtual std::vector<Worker *> make_workers();
        virtual std::vector<BackupBenchWorker *> make_backup_workers();
        virtual void init_store(MemDB* &store);
				virtual void init_backup_store(MemDB* &store) {assert(false);}
        virtual void warmup_buffer(char *);
        virtual void bootstrap_with_rdma(RdmaCtrl *r) {
        }
      };

      // main in TPCE
      void TpceTest(int argc, char **argv) {
        // simple environment checks
        fprintf(stdout,"tpce benchmark bootstraping...\n");

        TpceMainRunner runner(nocc::oltp::config_file_name);
        runner.run();
        fprintf(stdout,"TPCE-ends\n");
      }

      void TpceMainRunner::init_store(MemDB* &store){
        assert(store == NULL);
        store = new MemDB();
        int meta_size = META_SIZE;
        store->AddSchema(BROKER, TAB_BTREE,sizeof(uint64_t), sizeof(broker::value),meta_size);
        store->AddSchema(TRADETYPE,TAB_BTREE,sizeof(uint64_t),sizeof(trade_type::value),meta_size);
        store->AddSchema(CHARGE,TAB_BTREE,sizeof(uint64_t),sizeof(charge::value),meta_size);
        store->AddSchema(EXCHANGE,TAB_BTREE,sizeof(uint64_t),sizeof(exchange::value),meta_size);
        /* sbtree and btree1  is a string b+ tree */
        store->AddSchema(SECURITY,TAB_BTREE1,2,sizeof(security::value),meta_size);
        store->AddSchema(LT,TAB_BTREE1,2, sizeof(last_trade::value),meta_size);

        store->AddSchema(HOLDING, TAB_BTREE1,5,sizeof(holding::value), meta_size);
        store->AddSchema(HOLDING_HIST, TAB_BTREE1,2,sizeof(holding_history::value), meta_size);
        store->AddSchema(HOLDING_SUM, TAB_BTREE1, 5 ,sizeof(holding_summary::value), meta_size);
        store->AddSchema(SETTLEMENT,  TAB_BTREE, sizeof(uint64_t),sizeof(settlement::value), meta_size);
        store->AddSchema(CASH_TX, TAB_BTREE,sizeof(uint64_t),sizeof(cash_transaction::value),meta_size);
        store->AddSchema(TRADE_HIST,TAB_BTREE1,3,sizeof(trade_history::value),meta_size);
        store->AddSchema(TRADE,TAB_BTREE,sizeof(uint64_t),sizeof(trade::value),meta_size);

        store->AddSchema(CUSTACCT,TAB_BTREE,sizeof(uint64_t),sizeof(customer_account::value),meta_size);
        store->AddSchema(ACCTPER,TAB_BTREE1,5,sizeof(account_permission::value),meta_size);
        store->AddSchema(ECUST, TAB_BTREE, sizeof(uint64_t),sizeof(customers::value),meta_size);
        store->AddSchema(CUST_ACCT, TAB_BTREE1,2,sizeof(uint64_t),meta_size);
        store->AddSchema(COMPANY,TAB_BTREE,sizeof(uint64_t),sizeof(company::value),meta_size);
        store->AddSchema(COMPANY_C,TAB_BTREE1,3,sizeof(company_competitor::value),meta_size);

        store->AddSchema(CUST_TAX,TAB_BTREE,sizeof(uint64_t), sizeof(customer_taxrate::value),meta_size);
        store->AddSchema(CR,TAB_BTREE1, 4,sizeof(commission_rate::value),meta_size);
        store->AddSchema(TRADE_REQ,TAB_BTREE1,5,sizeof(trade_request::value),meta_size);

        store->AddSchema(DAILY_MARKET,TAB_BTREE1,4,sizeof(daily_market::value),meta_size);
        store->AddSchema(FINANCIAL, TAB_BTREE1,3,sizeof(financial::value),meta_size);
        store->AddSchema(NEWS_XREF, TAB_BTREE1,2,sizeof(news_xref::value),meta_size);

        /* secondary indexs */
        //      store->AddSchema(COMPANY_NAME,TAB_SBTREE,sizeof(uint64_t),sizeof(uint64_t) * 2,meta_size);
        store->AddSecondIndex(COMPANY_NAME,TAB_SBTREE,6);
        store->AddSecondIndex(SEC_IDX,TAB_SBTREE,5);
        store->AddSecondIndex(SEC_TAX_CUST,TAB_SBTREE,3);
        store->AddSecondIndex(SEC_CA_TRADE,TAB_SBTREE,3);
        store->AddSecondIndex(SEC_S_T, TAB_SBTREE, 5);
        store->AddSecondIndex(SEC_SC_CO, TAB_SBTREE, 2);
        store->AddSecondIndex(SEC_SC_TR, TAB_SBTREE, 5);
        store->AddSecondIndex(SEC_SC_INS,TAB_SBTREE, 4);

      }

      void TpceMainRunner::init_put() {
        // add some bundary records to the database
        uint64_t *max_key = new uint64_t[5];
        uint64_t *min_key = new uint64_t[5];
        for(uint i = 0; i < 5;++i) {
          max_key[i] = numeric_limits<uint64_t>::max();
          min_key[i] = 0;
        }
        uint64_t *dummy = new uint64_t;
        store_->Put(HOLDING,(uint64_t)max_key,dummy);
        store_->Put(HOLDING,(uint64_t)min_key,dummy);
        store_->Put(HOLDING_SUM,(uint64_t)min_key,dummy);
        store_->Put(HOLDING_SUM,(uint64_t)max_key,dummy);
        store_->Put(HOLDING_HIST,(uint64_t)max_key,dummy);
        store_->Put(CR,(uint64_t)max_key,dummy);
        store_->Put(CUST_ACCT,(uint64_t)max_key,dummy);
        store_->Put(TRADE_HIST,(uint64_t)max_key,dummy);
        store_->Put(TRADE_HIST,(uint64_t)min_key,dummy);
        store_->Put(COMPANY_C,(uint64_t)max_key,dummy);
        store_->Put(TRADE_REQ,(uint64_t)max_key,dummy);

        /* Single key case */
        uint64_t max_key1 = numeric_limits<uint64_t>::max();
        store_->Put(CUST_TAX,max_key1,dummy);

        store_->PutIndex(SEC_CA_TRADE,(uint64_t)max_key,dummy);
        store_->PutIndex(SEC_SC_CO,(uint64_t)max_key,dummy);
        store_->PutIndex(SEC_SC_TR,(uint64_t)max_key,dummy);
        store_->PutIndex(SEC_SC_INS,(uint64_t)max_key,dummy);
        store_->PutIndex(SEC_S_T,(uint64_t)max_key,dummy);

        delete max_key;
        delete min_key;
      }

      TpceMainRunner::TpceMainRunner (std::string &config_file)
        : BenchRunner(config_file)
      {
        //DBRad::GlobalInit();

        /* init parameters */
        // + 1 is now for test only
        totalCustomer = accountPerPartition * (total_partition );
        mStartCustomer = accountPerPartition * current_partition + 1;

        fprintf(stdout,"check constants cust-per-partition %d\n",accountPerPartition);

        /* IC3 uses 3 days, while ermia uses 10 days.
           Maybe we can set any of them
        */
        tradeDays = 1; /* it currently seems no problem to set it to 10 */
        scaleFactor = 500; /* default value */

        fprintf(stdout,"[TPCE setting ] total customer %d start from %d\n",totalCustomer,mStartCustomer);

        parse_workload(config_file);
      }

      std::vector<BenchLoader *> TpceMainRunner::make_loaders(int partition, MemDB* store) {
        std::vector<BenchLoader *> ret;
        /* The seed is not used in this loader, thus any number is ok */
        if(store == NULL){
          ret.push_back(new TpceLoader(73,store_));
        } else {
          ret.push_back(new TpceLoader(73,store));
        }
        return ret;
      }

      std::vector<Worker *> TpceMainRunner::make_workers() {
        fast_random r(23984543 + current_partition);
        std::vector<Worker *> ret;
        for(uint i = 0;i < nthreads;++i) {
          ret.push_back(new TpceWorker(i,r.next(),store_,&barrier_a_,&barrier_b_,this));
        }
#if SI_TX
        // add ts worker
        ts_manager = new TSManager(nthreads + nclients + 1,cm,0,0);
        //ret.push_back(ts_manager);
#ifndef EM_OCC
        ret.push_back(ts_manager);
#endif
#endif
#if CS == 1
        for(uint i = 0;i < nclients;++i)
          ret.push_back(new TpceClient(nthreads + i,r.next()));
#endif
        return ret;
      }

      std::vector<BackupBenchWorker *> TpceMainRunner::make_backup_workers() {
        return std::vector<BackupBenchWorker *> ();
      }

      void TpceMainRunner::warmup_buffer(char *buffer) {
      }

    };

  };
};
