#include "tpce_worker.h"
#include "tpce_loader_factory.h"

#include "db/txs/dbtx.h"

/* leverages this for TPCE file loading */
#include "egen/EGenLoader_stdafx.h"
#include "egen/EGenGenerateAndLoad.h"

/* It seems that in TPC-E, the trades's related data will all be in itself */

#include <set>
#include <map>

#undef  SANITY_CHECKS
//#define SANITY_CHECKS

using namespace std;
using namespace TPCE;

extern int verbose;
extern size_t current_partition;

namespace nocc {

  namespace oltp {

    namespace tpce {

      uint64_t lastTradeId(2); /* trade id start from 2 */
      extern CInputFiles *inputFiles;
      extern CEGenLogger *logger;

      extern int totalCustomer;
      extern int scaleFactor;
      extern int mStartCustomer;
      extern int tradeDays;
      extern const int accountPerPartition;

      /* used for sanity checks */
#ifdef SANITY_CHECKS
      DBRad *tx;
#endif
      CLogFormatTab fmt;

      std::map<int32_t,double> TpceTaxMap;
    
      std::map<std::string,int> TpceExchangeMap;
      std::map<std::string,uint64_t> SecurityToCompany;
      std::map<std::string,uint64_t> CONameToId;
      std::map<std::string,uint64_t> TpceTradeTypeMap;
    
      /* i think this table is static, thus no need to really store it in database */
      std::map<std::string,int64_t  > BrokerNameToKey;
      std::map<std::string,uint64_t > IndustryNametoID;

      /* TPCE fixed tables */
      std::map<std::string,zip_code::value *>     TpceZipCode;
      std::map<std::string,trade_type::value *>   TpceTradeHash;
      std::map<std::string,industry::value *>     TpceIndustry;
      std::map<uint64_t,news_item::value *>       TpceNewsItem;
      std::map<uint64_t,address::value *>         TpceAddress;
      std::map<std::string,status_type::value *>  TpceStatusType;
      std::map<std::string,std::string>           TpceSector;
      std::map<std::string,std::string>           SecurityToSector;
      std::map<uint64_t,uint64_t>                 WatchList;
      std::map<uint64_t,std::vector<std::string> >WatchItem;
    
    
      bool TypeMapInitDone = false;

      uint64_t preload_trade_per_server_offset(0);
  
      TpceLoader::TpceLoader(unsigned long seed, MemDB *store)
        : TpceMixin(store),
          BenchLoader(seed)
      {
    
      }
  
      void TpceLoader::load() {
    
        NoccTpceLoadFactory *factory = new NoccTpceLoadFactory(store_);
        /* Init TPCE loading factory */
    
        char  szInDir[64];
        //snprintf(szInDir,64,"/home/wxd/egen_flat_in/"); // used in local data center
        snprintf(szInDir,64,"/home/ubuntu/egen_flat_in/"); // used in aws
        int   iLoadUnitSize = iDefaultLoadUnitSize;
        bool  bGenerateUsingCache  = true;
    
        char szLogFileName[64];
    
        snprintf(&szLogFileName[0], sizeof(szLogFileName),
                 "NoccRunningFrom%" PRId64 "To%" PRId64 ".log",
                 (long long int)mStartCustomer, (long long int)((mStartCustomer + accountPerPartition) - 1));
        /* Create log formatter and logger instance */
        logger = new CEGenLogger(eDriverEGenLoader, 0, szLogFileName, &fmt);

        CGenerateAndLoadStandardOutput* output = NULL;
    
        inputFiles = new CInputFiles();
        output = new CGenerateAndLoadStandardOutput();
        inputFiles->Initialize(eDriverEGenLoader, totalCustomer, totalCustomer, szInDir);

        cerr << "[tpce] load settings:" << endl;
        cerr << "  Total customer    : " << totalCustomer << endl;
        cerr << "  Load unit size    : " << iLoadUnitSize  << endl;

    
        CGenerateAndLoad *TpceLoadFactory = new CGenerateAndLoad(*inputFiles, accountPerPartition,
                                                                 mStartCustomer,
                                                                 totalCustomer, iLoadUnitSize,
                                                                 scaleFactor, tradeDays,
                                                                 factory, logger, output, szInDir,
                                                                 bGenerateUsingCache);
#ifdef SANITY_CHECKS
        tx = new DBRad(store_,0,NULL);
#endif

        /* true loading */

        /* fixed tables */
        fprintf(stdout,"Generate and load fixed tables\n");
        TpceLoadFactory->m_iStartFromCustomer = 1;
        TpceLoadFactory->m_iCustomerCount = totalCustomer;
        TpceLoadFactory->GenerateAndLoadZipCode();
        TpceLoadFactory->GenerateAndLoadAddress();
        /* reset load range */
        TpceLoadFactory->m_iStartFromCustomer = mStartCustomer;
        TpceLoadFactory->m_iCustomerCount = accountPerPartition;
        /* reset done */
      
        /**/
      
        TpceLoadFactory->GenerateAndLoadTradeType();
        TpceLoadFactory->GenerateAndLoadCharge();
        TpceLoadFactory->GenerateAndLoadExchange();
        TpceLoadFactory->GenerateAndLoadStatusType();

        /* load all security to check for remote and local things */
        /* these two tables are replicated everywhere */
        TpceLoadFactory->m_iStartFromCustomer = 1;
        TpceLoadFactory->m_iCustomerCount = totalCustomer;

        TpceLoadFactory->GenerateAndLoadIndustry();
        TpceLoadFactory->GenerateAndLoadCompany();
        TpceLoadFactory->GenerateAndLoadSecurity();

        /* security related tables */
        TpceLoadFactory->GenerateAndLoadFinancial();
        TpceLoadFactory->GenerateAndLoadDailyMarket();
        TpceLoadFactory->GenerateAndLoadLastTrade();
        TpceLoadFactory->GenerateAndLoadNewsItemAndNewsXRef();
      
        /* reset load range */
        TpceLoadFactory->m_iStartFromCustomer = mStartCustomer;
        TpceLoadFactory->m_iCustomerCount = accountPerPartition;
        /* reset done */
      
        TpceLoadFactory->GenerateAndLoadTaxrate();
        TpceLoadFactory->GenerateAndLoadCommissionRate();
      
        TpceLoadFactory->GenerateAndLoadCustomerTaxrate();
      
        fprintf(stdout,"Generate and load customer account\n");
        TpceLoadFactory->GenerateAndLoadCustomer();
        TpceLoadFactory->GenerateAndLoadCustomerAccountAndAccountPermission();
      
        fprintf(stdout,"Generate trades table\n");
        TpceLoadFactory->GenerateAndLoadGrowingTables();
        //      TpceLoadFactory->GenerateAndLoadTaxrate
        fprintf(stdout,"\ntrades table done\n");

        /* Some remaining fixed tables */
        TpceLoadFactory->GenerateAndLoadCompanyCompetitor();
        TpceLoadFactory->GenerateAndLoadSector();
        TpceLoadFactory->GenerateAndLoadWatchListAndWatchItem();
      }

      class TpceWatchListLoader : public CBaseLoader<WATCH_LIST_ROW> {
      public:
        TpceWatchListLoader() {}
        virtual void WriteNextRecord(CBaseLoader<WATCH_LIST_ROW>::PT record) {
          assert(WatchList.find(record->WL_C_ID) == WatchList.end());
          WatchList.insert(std::make_pair(record->WL_C_ID,record->WL_ID));
          assert(WatchItem.find(record->WL_ID) == WatchItem.end());
          std::vector<std::string> empty_list;
          WatchItem.insert(std::make_pair(record->WL_ID,empty_list));
        }
        virtual void FinishLoad() { fprintf(stdout,"[TPCE loader] watch list load done.\n");}
      };

      class TpceWatchItemLoader : public CBaseLoader<WATCH_ITEM_ROW> {
      public:
        TpceWatchItemLoader() {}
        virtual void WriteNextRecord(CBaseLoader<WATCH_ITEM_ROW>::PT record) {
          assert(WatchItem.find(record->WI_WL_ID) != WatchItem.end());
          WatchItem[record->WI_WL_ID].push_back(record->WI_S_SYMB);
        }
        virtual void FinishLoad() { fprintf(stdout,"[TPCE loader] watch item load done.\n");}
      };
    

      class TpceSectorLoader : public CBaseLoader<SECTOR_ROW>, public TpceMixin {
      public:
        uint64_t *dummy;
        TpceSectorLoader(MemDB *db) : TpceMixin(db) {dummy = new uint64_t;}
        virtual void WriteNextRecord(CBaseLoader<SECTOR_ROW>::PT record) {
          uint64_t key = makeSectorKey(record->SC_NAME,record->SC_ID);
          delete (uint64_t *)key;
          TpceSector.insert(std::make_pair(record->SC_NAME,record->SC_ID));
        }
        virtual void FinishLoad() {}
      };

      class TpceAddressLoader   : public CBaseLoader<ADDRESS_ROW> , public TpceMixin {
      public:
        TpceAddressLoader(MemDB *db) : TpceMixin(db) {}
        virtual void WriteNextRecord(CBaseLoader<ADDRESS_ROW>::PT record) {
          address::value *v = new address::value;
          v->ad_line1   = std::string(record->AD_LINE1);
          v->ad_line2   = std::string(record->AD_LINE2);
          v->ad_zc_code = std::string(record->AD_ZC_CODE);
          v->ad_ctry    = std::string(record->AD_CTRY);
          assert(TpceZipCode.find(record->AD_ZC_CODE) != TpceZipCode.end());
          assert(TpceAddress.find(record->AD_ID) == TpceAddress.end());
          TpceAddress.insert(std::make_pair(record->AD_ID,v));
        }
        virtual void FinishLoad() {}
      };
    
      class TpceTradeTypeLoader : public CBaseLoader<TRADE_TYPE_ROW> , public TpceMixin {
        int index;
      public:
        TpceTradeTypeLoader(MemDB *store)
          :TpceMixin(store),index(1)
        {

        };
        virtual void WriteNextRecord(CBaseLoader<TRADE_TYPE_ROW>::PT record) {

          int tt_size = store_->_schemas[TRADETYPE].total_len;
          int meta_len = store_->_schemas[TRADETYPE].meta_len;
          char *tt_wrapper = (char *)malloc(tt_size);
          memset(tt_wrapper, 0, meta_len + sizeof(trade_type::value));
      
          trade_type::value *v = (trade_type::value *)(tt_wrapper + meta_len);
          v->tt_name = string(record->TT_NAME);
          v->tt_is_sell = record->TT_IS_SELL;
          v->tt_is_mrkt = record->TT_IS_MRKT;
      
          store_->Put(TRADETYPE,index,(uint64_t *)tt_wrapper);
	
          std::string key = std::string(record->TT_ID);

          ASSERT_PRINT(TpceTradeTypeMap.find(key) == TpceTradeTypeMap.end(),
                       stdout,"inserted key %s, %s\n",key.c_str(),v->tt_name);
          TpceTradeTypeMap.insert(std::make_pair(key,index++));
          TpceTradeHash.insert(std::make_pair(key,v));
        }
    
        virtual void FinishLoad() {      }
    
      };

      class TpceDMLoader : public CBaseLoader<DAILY_MARKET_ROW> , public TpceMixin {
      public:
        TpceDMLoader(MemDB *store) : TpceMixin(store) {} ;
        virtual void WriteNextRecord(CBaseLoader<DAILY_MARKET_ROW>::PT record) {
	
          int dm_size = store_->_schemas[DAILY_MARKET].total_len;
          int meta_len = store_->_schemas[DAILY_MARKET].meta_len;
          char *wrapper = (char *)malloc(dm_size + meta_len);

          daily_market::value *v = (daily_market::value *)(wrapper + meta_len);
#if 0
          v->dm_close = record->DM_CLOSE;
          v->dm_high  = record->DM_HIGH;
          v->dm_low   = record->DM_LOW;
          v->dm_vol   = record->DM_VOL;
#endif
          uint64_t dm_key = makeDMKey(record->DM_S_SYMB,record->DM_DATE.GetDate());

          store_->Put(DAILY_MARKET,dm_key,(uint64_t *)wrapper);
          delete (uint64_t *)dm_key;
        }
        virtual void FinishLoad() {}
      };

      class TpceFinLoader : public CBaseLoader<FINANCIAL_ROW>, public TpceMixin {
      public:
        TpceFinLoader(MemDB *store) : TpceMixin(store) {};
        virtual void WriteNextRecord(CBaseLoader<FINANCIAL_ROW>::PT record) {
          uint64_t f_key = makeFinKey(record->FI_CO_ID,record->FI_YEAR,record->FI_QTR);

          int meta_size = store_->_schemas[FINANCIAL].meta_len;
          int vlen      = store_->_schemas[FINANCIAL].vlen;
          char *wrapper = (char *)malloc(vlen + meta_size);

          financial::value *v = (financial::value *)(wrapper + meta_size);
          v->fi_qtr_start_date = record->FI_QTR_START_DATE.GetDate();
          v->fi_revenue        = record->FI_REVENUE;
          v->fi_net_earn       = record->FI_NET_EARN;
          v->fi_basic_eps      = record->FI_BASIC_EPS;
          v->fi_dilut_eps      = record->FI_DILUT_EPS;
          v->fi_margin         = record->FI_MARGIN;
          v->fi_inventory      = record->FI_INVENTORY;
          v->fi_assets         = record->FI_ASSETS;
          v->fi_liability      = record->FI_LIABILITY;
          v->fi_out_basic      = record->FI_OUT_BASIC;
          v->fi_out_dilut      = record->FI_OUT_DILUT;

          store_->Put(FINANCIAL,f_key,(uint64_t *)wrapper);
          delete (uint64_t *)f_key;
        }
        virtual void FinishLoad() {}
      };

      class TpceIndustryLoader : public CBaseLoader<INDUSTRY_ROW>, public TpceMixin {
        uint64_t local_counter;
      public:
        TpceIndustryLoader(MemDB *store) : TpceMixin(store) { local_counter = 73; };
        virtual void WriteNextRecord(CBaseLoader<INDUSTRY_ROW>::PT record) {
          industry::value *v = new industry::value;
          v->in_name = std::string(record->IN_NAME);
          v->in_sc_id = std::string(record->IN_SC_ID);
          assert(TpceIndustry.find(record->IN_ID) == TpceIndustry.end());
          TpceIndustry.insert(std::make_pair(record->IN_ID,v));
          assert(IndustryNametoID.find(record->IN_NAME) == IndustryNametoID.end());
          IndustryNametoID.insert(std::make_pair(record->IN_NAME,local_counter++));
        }
        virtual void FinishLoad() {}
      };

      class TpceNXRLoader : public CBaseLoader<NEWS_XREF_ROW>, public TpceMixin {
      public:
        TpceNXRLoader(MemDB *store) : TpceMixin(store) {} ;
        virtual void WriteNextRecord(CBaseLoader<NEWS_XREF_ROW>::PT record) {
          uint64_t xr_key = makeNXRKey(record->NX_CO_ID,record->NX_NI_ID);
          char *wrapper = (char *)malloc(sizeof(news_xref::value) +
                                         store_->_schemas[NEWS_XREF].meta_len);
          store_->Put(NEWS_XREF,xr_key,(uint64_t *)wrapper);
          delete (uint64_t *)xr_key;
        }
        virtual void FinishLoad() {}
      };
    
      class TpceNILoader : public CBaseLoader<NEWS_ITEM_ROW>, public TpceMixin {
      public:
        TpceNILoader(MemDB *store) : TpceMixin(store) { };
        virtual void WriteNextRecord(CBaseLoader<NEWS_ITEM_ROW>::PT record) {
          news_item::value *v = new news_item::value;
#if COMPRESS == 0
          v->ni_headline = record->NI_HEADLINE;
          v->ni_summary  = record->NI_SUMMARY;
          //	v->ni_item     = record->NI_ITEM;
          v->ni_dts      = record->NI_DTS.GetDate();
          v->ni_source   = record->NI_SOURCE;
          v->ni_author   = record->NI_AUTHOR;
#endif
          assert(TpceNewsItem.find(record->NI_ID) == TpceNewsItem.end());
          TpceNewsItem.insert(std::make_pair(record->NI_ID,v));
        }
        virtual void FinishLoad() {}
      };

      class TpceStatusTypeLoader : public CBaseLoader<STATUS_TYPE_ROW>,public TpceMixin {
      public:
        TpceStatusTypeLoader(MemDB *store ) : TpceMixin(store) {} ;
        virtual void WriteNextRecord(CBaseLoader<STATUS_TYPE_ROW>::PT record) {
          assert(TpceStatusType.find(record->ST_ID) == TpceStatusType.end());
          status_type::value *v = (status_type::value *)malloc(sizeof(status_type::value));
          v->st_name = std::string(record->ST_NAME);
          TpceStatusType.insert(std::make_pair(record->ST_ID,v));
        }
        virtual void FinishLoad() {}
      };
    
      class TpceZipCodeLoader : public CBaseLoader<ZIP_CODE_ROW>, public TpceMixin {
      public:
        TpceZipCodeLoader(MemDB *store) : TpceMixin(store) {} ;
        virtual void WriteNextRecord(CBaseLoader<ZIP_CODE_ROW>::PT record) {
          zip_code::value *v = new zip_code::value;
#if COMPRESS == 0
          v->zc_town = std::string(record->ZC_TOWN);
          v->zc_div  = std::string(record->ZC_DIV);
#endif
          assert(TpceZipCode.find(record->ZC_CODE) == TpceZipCode.end());
          TpceZipCode.insert(std::make_pair(record->ZC_CODE,v));
        }
        virtual void FinishLoad() {}
      };

      class TpceCompanyCLoader : public CBaseLoader<COMPANY_COMPETITOR_ROW>, public TpceMixin {
      public:
        TpceCompanyCLoader(MemDB *store) : TpceMixin(store) {} ;
        virtual void WriteNextRecord(CBaseLoader<COMPANY_COMPETITOR_ROW>::PT record) {
          uint64_t cc_key = makeCCKey(record->CP_CO_ID,record->CP_COMP_CO_ID,record->CP_IN_ID);
          int len = store_->_schemas[CUST_TAX].total_len;
          int meta = store_->_schemas[CUST_TAX].meta_len;
          char *dummy_v = (char *)malloc(len + meta);
	
          store_->Put(COMPANY_C,cc_key,(uint64_t *)dummy_v);
          assert(store_->Get(COMPANY,record->CP_CO_ID) != NULL);
          assert(store_->Get(COMPANY,record->CP_COMP_CO_ID) != NULL);
          delete (uint64_t *)cc_key;
        }
        virtual void FinishLoad() {}
      };

      class TpceChargeLoader : public CBaseLoader<CHARGE_ROW> , public TpceMixin {
      public:
        TpceChargeLoader(MemDB *store)
          :TpceMixin(store)
        {

        };
        virtual void WriteNextRecord(CBaseLoader<CHARGE_ROW>::PT record) {
      
          auto it = TpceTradeTypeMap.find(std::string(record->CH_TT_ID));
          assert(it != TpceTradeTypeMap.end());
          int tt_idx = it->second;

          //	fprintf(stdout,"name %s, idx %d\n",record->CH_TT_ID,tt_idx);
          /* Preparing data and key */
          int64_t c_key = makeChargeKey(tt_idx, record->CH_C_TIER);
          int ch_size = store_->_schemas[CHARGE].total_len;
          int meta_len = store_->_schemas[CHARGE].meta_len;
          char *ch_wrapper = (char *)malloc(ch_size);

          /*setting the data */
          charge::value *v = (charge::value *)(ch_wrapper + meta_len);
          v->ch_chrg = record->CH_CHRG;
          //      fprintf(stdout,"%f\n",v->ch_chrg);
          store_->Put(CHARGE,c_key,(uint64_t *)ch_wrapper);
          //delete (uint64_t *)c_key;
        }
    
        virtual void FinishLoad() {
          /* The first some implemented loader has sanity checks */
        }
    
      };

  
      class TpceExchangeLoader : public CBaseLoader<EXCHANGE_ROW> , public TpceMixin {
        int index;
      public:
        TpceExchangeLoader(MemDB *store)
          :TpceMixin(store),
           index(1) { }
      
        virtual void WriteNextRecord(CBaseLoader<EXCHANGE_ROW>::PT record) {
          auto it = TpceExchangeMap.find(std::string(record->EX_ID));
          assert(it == TpceExchangeMap.end());

          int ex_size = store_->_schemas[EXCHANGE].total_len;
          int meta_len = store_->_schemas[EXCHANGE].meta_len;
          char *ex_wrapper = (char *)malloc(ex_size);
          memset(ex_wrapper, 0, meta_len + sizeof(exchange::value));
      
          exchange::value *v = (exchange::value *)(ex_wrapper + meta_len);
          v->ex_name = record->EX_NAME;
          v->ex_num_symb = record->EX_NUM_SYMB;
          v->ex_open     = record->EX_OPEN;
          v->ex_close    = record->EX_CLOSE;
          v->ex_desc     = record->EX_DESC;
          v->ex_ad_id    = record->EX_AD_ID;

          store_->Put(EXCHANGE,index,(uint64_t *)ex_wrapper);

          TpceExchangeMap.insert(std::make_pair(std::string(record->EX_ID),index++));
        }

        virtual void FinishLoad() {
#if 0
          DBRad tx(store_,0,NULL);
          fprintf(stdout,"start sanity check exchange...\n");
          tx.reset();
      
          /* The first some implemented loader has sanity checks */
          for(auto it = TpceExchangeMap.begin(); it != TpceExchangeMap.end();++it) {
            fprintf(stdout,"exchange %s -> %d\n",it->first.c_str(),it->second);

            exchange::value *ev;
            uint64_t seq = tx.get(EXCHANGE,it->second,(char **)(&ev),sizeof(exchange::value));
            assert(seq == 2);
            fprintf(stdout,"exchange name %s\n",ev->ex_name.data());
          }
#endif      
        }
      
      };

      class TpceSecurityLoader : public CBaseLoader<SECURITY_ROW>, public TpceMixin {
        int loaded;
        uint64_t *dummy;
      public:
        TpceSecurityLoader(MemDB *store)
          : TpceMixin(store) {
          loaded = 0;
          dummy = new uint64_t;
        }
        virtual void WriteNextRecord(CBaseLoader<SECURITY_ROW>::PT record) {
      
          std::string symbl = record->S_SYMB;
          uint64_t sec_key = makeSecurityIndex(symbl);
      
          int se_size = store_->_schemas[SECURITY].total_len;
          int meta_len = store_->_schemas[SECURITY].meta_len;
          char *se_wrapper = (char *)malloc(se_size);
          memset(se_wrapper, 0, meta_len + sizeof(security::value));
      
          security::value *v = (security::value *)(se_wrapper + meta_len);
          v->s_issue = std::string(record->S_ISSUE);
          v->s_st_id = std::string(record->S_ST_ID);
          v->s_name  = std::string(record->S_NAME);
          v->s_ex_id = std::string(record->S_EX_ID);
          v->s_co_id = record->S_CO_ID;
          v->s_num_out = record->S_NUM_OUT;
          v->s_start_date   = (record->S_START_DATE).GetDate();
          v->s_exch_date    = record->S_EXCH_DATE.GetDate();
          // TODO !!
          /* ... some other fields, added on demand */

          store_->Put(SECURITY,sec_key,(uint64_t *)se_wrapper);
          loaded += 1;
	
          /* add am secondary index */
          /* first check whether an existing entry exists */
          uint64_t sec_idx_key = makeSecuritySecondIndex(v->s_co_id,record->S_ISSUE);
          uint64_t *mn = (uint64_t *)store_->_indexs[SEC_IDX]->Get(sec_idx_key);
          /* Ensuring no previous inserted index here */
          assert(mn == NULL);
	
          uint64_t *val = new uint64_t[5];
          memset(val,0,sizeof(uint64_t) * 5);
          memcpy(val,symbl.c_str(),symbl.size());
          store_->PutIndex(SEC_IDX,sec_idx_key,val);

          /* add a new mapping */
          assert(SecurityToCompany.find(std::string(record->S_SYMB)) == SecurityToCompany.end());
          SecurityToCompany.insert(std::make_pair(std::string(record->S_SYMB),record->S_CO_ID));
          assert(SecurityToSector.find(std::string(record->S_SYMB)) == SecurityToSector.end());

          /* get sec id from company=>industry */
          uint64_t *tmp_val  = store_->Get(COMPANY,v->s_co_id);
          assert(tmp_val != NULL);
          company::value *cov = (company::value *)((char *)tmp_val + meta_len);
          assert(TpceIndustry.find(cov->co_in_id.data()) != TpceIndustry.end());
          industry::value *inv = TpceIndustry[cov->co_in_id.data()];
          SecurityToSector.insert(std::make_pair(record->S_SYMB,inv->in_sc_id.data()));

          /* add another sec index for the ease of market watch industry scan */
          assert(IndustryNametoID.find(inv->in_name.data()) != IndustryNametoID.end());
          uint64_t in_co_s_key = makeSecondCompanyIndustrySecurity(IndustryNametoID[inv->in_name.data()],
                                                                   v->s_co_id,record->S_SYMB);
          //	if(IndustryNametoID[inv->in_name.data()] == 154 && v->s_co_id > 4300000001) {
          //	  fprintf(stdout,"got one, co_id %lu\n",v->s_co_id);
          //	}
          store_->PutIndex(SEC_SC_INS,in_co_s_key,dummy); /* static table, put dummy is ok */
	
          delete (uint64_t *)sec_key;
          delete (uint64_t *)sec_idx_key;
          delete (uint64_t *)in_co_s_key;
        }
    
        virtual void FinishLoad () {
          fprintf(stdout,"loading security done, sizeof the record %lu, total loaded %d\n",
                  sizeof(security::value),loaded);
        }
      };

      class TpceTRLoader : public CBaseLoader<TRADE_REQUEST_ROW>, public TpceMixin {
      public:
        TpceTRLoader(MemDB *store) : TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<TRADE_REQUEST_ROW>::PT record) {
          // pass
        }
        virtual void FinishLoad() {}
      };

      class TpceLTLoader : public CBaseLoader<LAST_TRADE_ROW>, public TpceMixin {
      public:
        TpceLTLoader(MemDB *store)
          : TpceMixin(store) {
        }
        virtual void WriteNextRecord(CBaseLoader<LAST_TRADE_ROW>::PT record) {
      
          std::string symbl = record->LT_S_SYMB;
          uint64_t sec_key = makeSecurityIndex(symbl);
	
          int se_size = store_->_schemas[LT].total_len;
          int meta_len = store_->_schemas[LT].meta_len;
          char *se_wrapper = (char *)malloc(se_size);
          memset(se_wrapper, 0, meta_len + sizeof(security::value));

          last_trade::value *v = (last_trade::value *)(se_wrapper + meta_len);
          v->lt_dts = record->LT_DTS.GetDate();
          v->lt_price = record->LT_PRICE;
          v->lt_open_price = record->LT_OPEN_PRICE;
          v->lt_vol = record->LT_VOL;
	
          store_->Put(LT,sec_key,(uint64_t *)se_wrapper);
          delete (uint64_t *)sec_key;
        }
    
        virtual void FinishLoad () {      }
      };
    
      class TpceTaxRateLoader : public CBaseLoader<TAXRATE_ROW>  {
        int index;
      public:
        TpceTaxRateLoader() {index = 0;}
        virtual void WriteNextRecord(CBaseLoader<TAXRATE_ROW>::PT record) {
          int32_t name = *((int32_t *)record->TX_ID);
          assert(TpceTaxMap.find(name) == TpceTaxMap.end());
          TpceTaxMap.insert(std::make_pair(name,record->TX_RATE));
        }
        virtual void FinishLoad() {}
      };
    
      class TpceCTLoader : public CBaseLoader<CUSTOMER_TAXRATE_ROW> , public TpceMixin {
      public:
        TpceCTLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<CUSTOMER_TAXRATE_ROW>::PT record) {
      
          int len = store_->_schemas[CUST_TAX].total_len;
          int meta = store_->_schemas[CUST_TAX].meta_len;
          char *wrapper = (char *)malloc(len + meta);

          customer_taxrate::value *v = (customer_taxrate::value *)(wrapper + meta);
          uint64_t key = makeCustTaxKey(record->CX_C_ID,*(uint64_t *)(record->CX_TX_ID));
          assert(TpceTaxMap.find(key & 0xffffffff) != TpceTaxMap.end());
          store_->Put(CUST_TAX,key,(uint64_t *)wrapper);
        }
        virtual void FinishLoad() {}
      };

      class TpceCRLoader : public CBaseLoader<COMMISSION_RATE_ROW> , public TpceMixin {
      public:
        TpceCRLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<COMMISSION_RATE_ROW>::PT record) {
	
          int len = store_->_schemas[CR].total_len;
          int meta = store_->_schemas[CR].meta_len;
          char *wrapper = (char *)malloc(len + meta);

          commission_rate::value *v = (commission_rate::value *)(wrapper + meta);
          uint64_t key = makeCRKey(record->CR_C_TIER,record->CR_TT_ID,record->CR_EX_ID,
                                   record->CR_FROM_QTY);
          v->cr_to_qty = record->CR_TO_QTY;
          v->cr_rate   = record->CR_RATE;
          store_->Put(CR,key,(uint64_t *)wrapper);
          delete (uint64_t *)key;
        }
        virtual void FinishLoad() {}
      };
    

      class TpceTradeLoader : public CBaseLoader<TRADE_ROW> , public TpceMixin {
        uint64_t *dummy;
      public:
        TpceTradeLoader(MemDB *store) :
          TpceMixin(store) { dummy = new uint64_t;
          preload_trade_per_server_offset = (INT64)(( (INT64)(HoursPerWorkDay * tradeDays) * SecondsPerHour *
                                                      /* 1.01 to account for rollbacks */
                                                      (accountPerPartition / scaleFactor )) * iAbortTrade / INT64_CONST(100) );
          preload_trade_per_server_offset *= current_partition;
          fprintf(stdout,"[TRADE] trade loader init, sizeof record %lu\n",sizeof(trade::value));
        }
        virtual void WriteNextRecord(CBaseLoader<TRADE_ROW>::PT record) {
	
          int len = store_->_schemas[TRADE].total_len;
          int meta = store_->_schemas[TRADE].meta_len;
          char *wrapper = (char *)malloc(meta + len);
          memset(wrapper,0,len + meta);
	
          trade::value *v = (trade::value *)(wrapper + meta);
	
          if(iAbortedTradeModFactor == lastTradeId % iAbortTrade)
            lastTradeId += 1;
          /* currently manally create trade id */
          //uint64_t tid = lastTradeId + iTTradeShift;
          //	lastTradeId += 1;
          uint64_t tid = record->T_ID;
          tid -= preload_trade_per_server_offset;
          if(tid > lastTradeId)
            lastTradeId = tid;
          //	if(tid == 200000000054562) fprintf(stdout,"load!!!\n");
          tid = encode_trade_id(tid,0,0);
          v->t_dts = record->T_DTS.GetDate();
          v->t_st_id = std::string(record->T_ST_ID);
          v->t_tt_id = std::string(record->T_TT_ID);
          v->t_is_cash = record->T_IS_CASH;
          v->t_s_symb  = std::string(record->T_S_SYMB);
          v->t_qty     = record->T_QTY;
          v->t_bid_price = record->T_BID_PRICE;
          v->t_ca_id     = record->T_CA_ID;
          v->t_exec_name = std::string(record->T_EXEC_NAME);
          v->t_trade_price = record->T_TRADE_PRICE;
          v->t_chrg = record->T_CHRG;
          v->t_comm = record->T_COMM;
          v->t_tax  = record->T_TAX;
          v->t_lifo = record->T_LIFO;
          assert(record->T_ID != 0);
	
          assert(v->t_is_cash == record->T_IS_CASH);
          store_->Put(TRADE,tid,(uint64_t *)wrapper);
          //      assert(ca_id_norm <= 50000);
#if 0      
          /* sanity checks */
          tx->reset();
          security::value *vs;
          std::string test = std::string(record->T_S_SYMB);
      
          uint64_t *key = new uint64_t[5];
          memset((char *)key,0,sizeof(uint64_t) * 5);
          memcpy(key,test.c_str(),test.size());
      
          uint64_t seq = tx->get(SECURITY,(uint64_t)key,(char **)(&vs),sizeof(security::value));
          assert(seq == 2);
#endif
          /* ca->trade sec index */
          uint64_t t_sec_key = makeSecondCATrade(v->t_ca_id,v->t_dts,tid);
          uint64_t *test = store_->GetIndex(SEC_CA_TRADE,t_sec_key);
          assert(test == NULL);
          store_->PutIndex(SEC_CA_TRADE,t_sec_key,dummy);
          delete (uint64_t *)t_sec_key;

          /* security->trade sec index */
          uint64_t s_sec_key = makeSecondSecTrade(record->T_S_SYMB,record->T_DTS.GetDate(),
                                                  tid);
          test = store_->GetIndex(SEC_S_T,s_sec_key);
          assert(test == NULL);
          store_->PutIndex(SEC_S_T,s_sec_key,dummy);
          delete (uint64_t *)s_sec_key;
        }
        virtual void FinishLoad() { fprintf(stdout,".");    }
      };

      class TpceHoldingLoader : public CBaseLoader<HOLDING_ROW> , public TpceMixin {
      public:
        TpceHoldingLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<HOLDING_ROW>::PT record) {

          std::string symbl = std::string(record->H_S_SYMB);
          //	if(symbl == "PFSW" && record->H_CA_ID == 43000026345)
          //	  fprintf(stdout,"loading %lu,%lu,%lu\n",record->H_CA_ID,record->H_T_ID,record->H_DTS.GetDate());
          uint64_t sec = makeHoldingKey(record->H_CA_ID,
                                        encode_trade_id(record->H_T_ID - preload_trade_per_server_offset,0,0),
                                        symbl,record->H_DTS.GetDate());
	
          int holding_sz = store_->_schemas[HOLDING].total_len;
          int meta_sz    = store_->_schemas[HOLDING].meta_len;
	
          char *holding_wrapper = (char *)malloc(holding_sz + meta_sz);
          memset(holding_wrapper,0,holding_sz + meta_sz);
	
          holding::value *v = (holding::value *)(holding_wrapper + meta_sz);
          v->h_dts = record->H_DTS.GetDate();
          v->h_price = record->H_PRICE;
          v->h_qty   = record->H_QTY;
          assert(store_->Get(HOLDING,sec) == NULL);
          store_->Put(HOLDING,sec,(uint64_t *)(holding_wrapper));

#ifdef SANITY_CHECKS
          {
            /* sanity checks */
            uint64_t sec = makeSecurityIndex(symbl);
            security::value *vs;
            tx->reset();
            uint64_t seq = tx->get(SECURITY,(uint64_t)sec,(char **)(&vs),sizeof(security::value));
            if(seq != 2) {
              fprintf(stdout,"symbl %s\n",symbl.c_str());
              assert(false);
            }
          }
#endif
          delete (uint64_t *)sec;
        }
        virtual void FinishLoad() { fprintf(stdout,".");}
      };
  
      class TpceHHLoader : public CBaseLoader<HOLDING_HISTORY_ROW> , public TpceMixin {
      public:
        TpceHHLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<HOLDING_HISTORY_ROW>::PT record) {
          int sz = store_->_schemas[HOLDING_HIST].total_len;
          int meta = store_->_schemas[HOLDING_HIST].meta_len;

          char *wrapper = (char *)malloc(sz + meta);
          memset(wrapper,0,sz + meta);

          holding_history::value *v = (holding_history::value *)(wrapper + meta);
          uint64_t key =
            makeHoldingHistKey(encode_trade_id(record->HH_T_ID - preload_trade_per_server_offset,0,0),
                               encode_trade_id(record->HH_H_T_ID - preload_trade_per_server_offset,0,0));
          v->hh_before_qty = record->HH_BEFORE_QTY;
          v->hh_after_qty  = record->HH_AFTER_QTY;

          store_->Put(HOLDING_HIST,key,(uint64_t *)wrapper);
          delete (uint64_t *)key;
        }
        virtual void FinishLoad() { }
      };

      class TpceHSLoader : public CBaseLoader<HOLDING_SUMMARY_ROW> , public TpceMixin {
      public:
        TpceHSLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<HOLDING_SUMMARY_ROW>::PT record) {
	
          std::string symb = record->HS_S_SYMB;
          uint64_t sec = makeHSKey(record->HS_CA_ID,symb);
      
          int len = store_->_schemas[HOLDING_SUM].total_len;
          int meta = store_->_schemas[HOLDING_SUM].meta_len;
          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);
      
          holding_summary::value *v = (holding_summary::value *)(wrapper + meta);
          v->hs_qty = record->HS_QTY;
          store_->Put(HOLDING_SUM,sec,(uint64_t *)wrapper);
      
#ifdef SANITY_CHECKS
          {
            uint64_t *keys = (uint64_t *)sec;
            tx->reset();
            customer_account::value *vc;
            uint64_t seq = tx->get(CUSTACCT,keys[0],(char **)(&vc),sizeof(customer_account::value));
            if(seq != 2) {
              fprintf(stdout,"Failed ca id %lu\n",keys[0]);
              assert(false);
            }
          }
#endif      
          delete (uint64_t *)sec;
        }
        virtual void FinishLoad() { fprintf(stdout,".");}
      };

      class TpceBrokerLoader : public CBaseLoader<BROKER_ROW> , public TpceMixin {
        int num;
      public:
        TpceBrokerLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<BROKER_ROW>::PT record) {
	
          int len = store_->_schemas[BROKER].total_len;
          int meta = store_->_schemas[BROKER].meta_len;
          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);

          broker::value *v = (broker::value *)(wrapper + meta);

          v->b_st_id = std::string(record->B_ST_ID);
          v->b_name  = std::string(record->B_NAME);
          v->b_num_trades = record->B_NUM_TRADES;
          v->b_comm_total = record->B_COMM_TOTAL;

          store_->Put(BROKER,record->B_ID,(uint64_t *)wrapper);
          num += 1;
          /* TODO!! maybe need secondary index? */
          assert(BrokerNameToKey.find(std::string(record->B_NAME)) == BrokerNameToKey.end());
          BrokerNameToKey.insert(std::make_pair(record->B_NAME,record->B_ID));
        }
        virtual void FinishLoad() {  fprintf(stdout,".");  }
      };

      class TpceSELoader : public CBaseLoader<SETTLEMENT_ROW> , public TpceMixin {
      public:
        TpceSELoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<SETTLEMENT_ROW>::PT record) {
	
          int len = store_->_schemas[SETTLEMENT].total_len;
          int meta = store_->_schemas[SETTLEMENT].meta_len;

          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);

          settlement::value *v = (settlement::value *)(wrapper + meta);
          v->se_cash_type = std::string(record->SE_CASH_TYPE);
          v->se_cash_due_date = record->SE_CASH_DUE_DATE.GetDate();
          v->se_amt = record->SE_AMT;
          store_->Put(SETTLEMENT,encode_trade_id(record->SE_T_ID - preload_trade_per_server_offset,0,0),
                      (uint64_t *)wrapper);
        }
        virtual void FinishLoad() { }
      };

      class TpceCTXLoader : public CBaseLoader<CASH_TRANSACTION_ROW> , public TpceMixin {
      public:
        TpceCTXLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<CASH_TRANSACTION_ROW>::PT record) {

          int len = store_->_schemas[CASH_TX].total_len;
          int meta = store_->_schemas[CASH_TX].meta_len;

          char *wrapper = (char *)(malloc(meta + len));
          memset(wrapper,0,meta + len);
	
          cash_transaction::value *v = (cash_transaction::value *)(wrapper + meta);
          v->ct_dts = record->CT_DTS.GetDate();
          v->ct_amt = record->CT_AMT;
          v->ct_name = std::string(record->CT_NAME);
          store_->Put(CASH_TX,
                      encode_trade_id(record->CT_T_ID - preload_trade_per_server_offset,0,0),
                      (uint64_t *)wrapper);
          // TODO add some secondary indexes
        }
        virtual void FinishLoad() { }
      };

      class TpceTradeHLoader : public CBaseLoader<TRADE_HISTORY_ROW> , public TpceMixin {
      public:
        TpceTradeHLoader(MemDB *store) :
          TpceMixin(store) {
          fprintf(stdout,"[TRADE HISTORY] started, sizeof record %lu\n",sizeof(trade_history::value));
        }
        virtual void WriteNextRecord(CBaseLoader<TRADE_HISTORY_ROW>::PT record) {

          int len = store_->_schemas[TRADE_HIST].total_len;
          int meta = store_->_schemas[TRADE_HIST].meta_len;
          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);
	
          trade_history::value *v = (trade_history::value *)(wrapper + meta);
          //	v->th_st_id = std::string(record->TH_ST_ID);

          /* make key */
          assert(record->TH_T_ID != 0);
          uint64_t sec = makeTHKey(encode_trade_id(record->TH_T_ID - preload_trade_per_server_offset,0,0),
                                   record->TH_DTS.GetDate(),record->TH_ST_ID);
          store_->Put(TRADE_HIST,sec,(uint64_t *)wrapper);
          delete (uint64_t *)sec;
        }
        virtual void FinishLoad() { }
      };


      class TpceCALoader : public CBaseLoader<CUSTOMER_ACCOUNT_ROW> , public TpceMixin {
      public:
        TpceCALoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<CUSTOMER_ACCOUNT_ROW>::PT record) {
          int len = store_->_schemas[CUSTACCT].total_len;
          int meta = store_->_schemas[CUSTACCT].meta_len;
          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);

          customer_account::value *v = (customer_account::value *)(wrapper + meta);
          v->ca_b_id = record->CA_B_ID;
          v->ca_c_id = record->CA_C_ID;
          v->ca_name = std::string(record->CA_NAME);
          v->ca_tax_st = record->CA_TAX_ST;
          v->ca_bal = record->CA_BAL;

          store_->Put(CUSTACCT,record->CA_ID,(uint64_t *)wrapper);
          if(caToPartition(record->CA_ID) != current_partition) {
            //	  fprintf(stdout,"ca id %lu custid %lu\n",record->CA_ID,v->ca_c_id);
            assert(false);
          }
          /* add an index for cust->ca mapping */
          uint64_t *dummy = new uint64_t;
          uint64_t key = makeCustAcctKey(v->ca_c_id,record->CA_ID);
          store_->Put(CUST_ACCT,key,dummy);
          delete (uint64_t *)key;
        }
        virtual void FinishLoad() { }
      };

      class TpceCompanyLoader : public CBaseLoader<COMPANY_ROW> , public TpceMixin {
      public:
        uint64_t *dummy;
        TpceCompanyLoader(MemDB *store) :
          TpceMixin(store) { dummy = new uint64_t;}
        virtual void WriteNextRecord(CBaseLoader<COMPANY_ROW>::PT record) {
          int len = store_->_schemas[COMPANY].total_len;
          int meta = store_->_schemas[COMPANY].meta_len;
          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);
	
          /* seems no secondary index is needed for customer */
          company::value *v = (company::value *)(wrapper + meta);
          //	v->co_st_id = std::string(record->CO_ST_ID);
          std::string name = std::string(record->CO_NAME);
          v->co_name = name;
          v->co_in_id = std::string(record->CO_IN_ID);
          v->co_sp_rate = std::string(record->CO_SP_RATE);
          //	v->co_ceo     = std::string(record->CO_CEO);
          v->co_ad_id   = record->CO_AD_ID;
          v->co_open_date = record->CO_OPEN_DATE.GetDate();

          /* This need secondary index on co_name */
          store_->Put(COMPANY,record->CO_ID,(uint64_t *)wrapper);
          assert(CONameToId.find(name) == CONameToId.end());
          assert(TpceAddress.find(record->CO_AD_ID) != TpceAddress.end());
          CONameToId.insert(std::make_pair(name,record->CO_ID));
#if 0			  
          char *s_wrapper = (char *)malloc(sizeof(uint64_t) * 2);
          uint64_t *prikeys = (uint64_t *)(s_wrapper);
          prikeys[0] = 1;
          prikeys[1] = record->CO_ID;
          /* currently using an b+tree, maybe we should use a more efficient b+ tree */
          store_->PutIndex(COMPANY_NAME,name_key,(uint64_t *)s_wrapper);
          delete (uint64_t *)name_key;
#endif

          /* add another index */
          assert(TpceIndustry.find(v->co_in_id.data()) != TpceIndustry.end());
          industry::value *inv = TpceIndustry[v->co_in_id.data()];
          uint64_t sec_key = makeSecondSecCompany(inv->in_sc_id.data(),companyToPartition(record->CO_ID));
          uint64_t *test = store_->GetIndex(SEC_SC_CO,sec_key);
          if(test == NULL)
            store_->PutIndex(SEC_SC_CO,sec_key,dummy);
          delete (uint64_t *)sec_key;
        }
        virtual void FinishLoad() { }
      };
    

      class TpceCustLoader : public CBaseLoader<CUSTOMER_ROW> , public TpceMixin {
      public:
        TpceCustLoader(MemDB *store) :
          TpceMixin(store) {}
        virtual void WriteNextRecord(CBaseLoader<CUSTOMER_ROW>::PT record) {
          int len = store_->_schemas[ECUST].total_len;
          int meta = store_->_schemas[ECUST].meta_len;
          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);

          /* seems no secondary index is needed for customer */
          customers::value *v = (customers::value *)(wrapper + meta);

          v->c_tax_id = string(record->C_TAX_ID);
          v->c_st_id  = string(record->C_ST_ID);
          v->c_l_name = string(record->C_L_NAME);
          v->c_f_name = string(record->C_F_NAME);
          v->c_m_name = string(record->C_M_NAME);
          v->c_gndr   = record->C_GNDR;
          v->c_tier   = record->C_TIER;
          v->c_dob    = record->C_DOB.GetDate();
          v->c_ad_id  = record->C_AD_ID;

          store_->Put(ECUST,record->C_ID,(uint64_t *)wrapper);

          /* add a secondary index */
          uint64_t tax_cust = makeTaxCustKey( std::string(record->C_TAX_ID),record->C_ID);
          uint64_t *dummy = new uint64_t;
          store_->PutIndex(SEC_TAX_CUST,tax_cust,dummy);
        }
        virtual void FinishLoad() { }
      };


      class TpceAPLoader : public CBaseLoader<ACCOUNT_PERMISSION_ROW> , public TpceMixin {
      public:
        TpceAPLoader(MemDB *store) :
          TpceMixin(store) {}
    
        virtual void WriteNextRecord(CBaseLoader<ACCOUNT_PERMISSION_ROW>::PT record) {
      
          int len = store_->_schemas[ACCTPER].total_len;
          int meta = store_->_schemas[ACCTPER].meta_len;
          char *wrapper = (char *)malloc(len + meta);
          memset(wrapper,0,len + meta);
      
          account_permission::value *v = (account_permission::value *)(wrapper + meta);
          v->ap_acl = std::string(record->AP_ACL);
          v->ap_l_name = std::string(record->AP_L_NAME);
          v->ap_f_name = std::string(record->AP_F_NAME);
          uint64_t key = makeAPKey(record->AP_CA_ID,std::string(record->AP_TAX_ID));
          store_->Put(ACCTPER,key,(uint64_t *)wrapper);
          delete (uint64_t *)key;
        }
        virtual void FinishLoad() { fprintf(stdout,"acct permission done\n");}
      };
  

      /* Some simple code.... */
      NoccTpceLoadFactory::NoccTpceLoadFactory (MemDB *store) {
        ttl_ = new TpceTradeTypeLoader(store);
        chl_ = new TpceChargeLoader(store);
        exl_ = new TpceExchangeLoader(store);
        sel_ = new TpceSecurityLoader(store);
        tl_  = new TpceTradeLoader(store);
        hol_ = new TpceHoldingLoader(store);
        hosl_ = new TpceHSLoader(store);
        hohl_ = new TpceHHLoader(store);
        brl_  = new TpceBrokerLoader(store);
        setl_  = new TpceSELoader(store);
        ctl_   = new TpceCTXLoader(store);
        thl_   = new TpceTradeHLoader(store);
        cal_   = new TpceCALoader(store);
        apl_   = new TpceAPLoader(store);
        cl_    = new TpceCustLoader(store);
        col_   = new TpceCompanyLoader(store);
        ltl_   = new TpceLTLoader(store);
        cutrl_ = new TpceCTLoader(store);
        taxl_  = new TpceTaxRateLoader();
        corl_  = new TpceCRLoader(store);
        trl_   = new TpceTRLoader(store);
        zcl_   = new TpceZipCodeLoader(store);
        ccl_   = new TpceCompanyCLoader(store);
        dml_   = new TpceDMLoader(store);
        finl_  = new TpceFinLoader(store);
        inl_   = new TpceIndustryLoader(store);
        nil_   = new TpceNILoader(store);
        nxrl_  = new TpceNXRLoader(store);
        addl_  = new TpceAddressLoader(store);
        stl_   = new TpceStatusTypeLoader(store);
        scl_   = new TpceSectorLoader(store);
        wll_   = new TpceWatchListLoader();
        wil_   = new TpceWatchItemLoader();
      }
    
      CBaseLoader<TRADE_TYPE_ROW> *NoccTpceLoadFactory::CreateTradeTypeLoader() {
        return ttl_;
      }
  
      CBaseLoader<CHARGE_ROW> *NoccTpceLoadFactory::CreateChargeLoader() {
        return chl_;
      }

      CBaseLoader<EXCHANGE_ROW> *NoccTpceLoadFactory::CreateExchangeLoader() {
        return exl_;
      }

      CBaseLoader<SECURITY_ROW>  *NoccTpceLoadFactory::CreateSecurityLoader() {
        return sel_;
      }

      CBaseLoader<TRADE_ROW>     *NoccTpceLoadFactory::CreateTradeLoader() {
        return tl_;
      }

      CBaseLoader<HOLDING_ROW>     *NoccTpceLoadFactory::CreateHoldingLoader() {
        return hol_;
      }

      CBaseLoader<HOLDING_HISTORY_ROW> *NoccTpceLoadFactory::CreateHoldingHistoryLoader() {
        return hohl_;
      }

      CBaseLoader<HOLDING_SUMMARY_ROW> *NoccTpceLoadFactory::CreateHoldingSummaryLoader() {
        return hosl_;
      }

      CBaseLoader<BROKER_ROW>  *NoccTpceLoadFactory::CreateBrokerLoader() {
        return brl_;
      }

      CBaseLoader<SETTLEMENT_ROW> *NoccTpceLoadFactory::CreateSettlementLoader() {
        return setl_;
      }

      CBaseLoader<CASH_TRANSACTION_ROW> *NoccTpceLoadFactory::CreateCashTransactionLoader() {
        return ctl_;
      }

      CBaseLoader<TRADE_HISTORY_ROW> *NoccTpceLoadFactory::CreateTradeHistoryLoader() {
        return thl_;
      }

      CBaseLoader<CUSTOMER_ACCOUNT_ROW>  *NoccTpceLoadFactory::CreateCustomerAccountLoader() {
        return cal_;
      }

      CBaseLoader<ACCOUNT_PERMISSION_ROW> *NoccTpceLoadFactory::CreateAccountPermissionLoader() {
        return apl_;
      }

      CBaseLoader<CUSTOMER_ROW> *NoccTpceLoadFactory::CreateCustomerLoader() {
        return cl_;
      }

      CBaseLoader<COMPANY_ROW> *NoccTpceLoadFactory::CreateCompanyLoader() {
        return col_;
      }
      CBaseLoader<LAST_TRADE_ROW> *NoccTpceLoadFactory::CreateLastTradeLoader() {
        return ltl_;
      }

      CBaseLoader<CUSTOMER_TAXRATE_ROW> *NoccTpceLoadFactory::CreateCustomerTaxrateLoader() {
        return cutrl_;
      }

      CBaseLoader<TAXRATE_ROW> *NoccTpceLoadFactory::CreateTaxrateLoader() {
        return taxl_;
      }
      CBaseLoader<COMMISSION_RATE_ROW> *NoccTpceLoadFactory::CreateCommissionRateLoader() {
        return corl_;
      }
      CBaseLoader<TRADE_REQUEST_ROW> *NoccTpceLoadFactory::CreateTradeRequestLoader() {
        return trl_;
      }
      CBaseLoader<ZIP_CODE_ROW> *NoccTpceLoadFactory::CreateZipCodeLoader() {
        return zcl_;
      }
      CBaseLoader<COMPANY_COMPETITOR_ROW> *NoccTpceLoadFactory::CreateCompanyCompetitorLoader() {
        return ccl_;
      }
      CBaseLoader<DAILY_MARKET_ROW > *NoccTpceLoadFactory::CreateDailyMarketLoader() {
        return dml_;
      }
      CBaseLoader<FINANCIAL_ROW> *NoccTpceLoadFactory::CreateFinancialLoader() {
        return finl_;
      }
      CBaseLoader<INDUSTRY_ROW> *NoccTpceLoadFactory::CreateIndustryLoader() {
        return inl_;
      }
      CBaseLoader<NEWS_ITEM_ROW> *NoccTpceLoadFactory::CreateNewsItemLoader() {
        return nil_;
      }
      CBaseLoader<NEWS_XREF_ROW> *NoccTpceLoadFactory::CreateNewsXRefLoader() {
        return nxrl_;
      }
      CBaseLoader<ADDRESS_ROW> *NoccTpceLoadFactory::CreateAddressLoader() {
        return addl_;
      }
      CBaseLoader<STATUS_TYPE_ROW> *NoccTpceLoadFactory::CreateStatusTypeLoader() {
        return stl_;
      }
      CBaseLoader<SECTOR_ROW> *NoccTpceLoadFactory::CreateSectorLoader() {
        return scl_;
      }
      CBaseLoader<WATCH_LIST_ROW> *NoccTpceLoadFactory::CreateWatchListLoader() {
        return wll_;
      }
      CBaseLoader<WATCH_ITEM_ROW> *NoccTpceLoadFactory::CreateWatchItemLoader() {
        return wil_;
      }
    };
  };
};
