#ifndef NOCC_OLTP_TPCE_LOAD_FACTORY_H
#define NOCC_OLTP_TPCE_LOAD_FACTORY_H

#include "memstore/memdb.h"
#include "egen/BaseLoaderFactory.h"

namespace nocc {
      namespace oltp {
            namespace tpce {

                  class NoccTpceLoadFactory : public TPCE::CBaseLoaderFactory {
                  public:
                        NoccTpceLoadFactory (MemDB *store);

                        /* done */
                        virtual TPCE::CBaseLoader<TPCE::CUSTOMER_ACCOUNT_ROW> *CreateCustomerAccountLoader();
                        virtual TPCE::CBaseLoader<TPCE::TRADE_HISTORY_ROW> *CreateTradeHistoryLoader();
                        virtual TPCE::CBaseLoader<TPCE::TRADE_ROW> *CreateTradeLoader();
                        virtual TPCE::CBaseLoader<TPCE::SECURITY_ROW> *CreateSecurityLoader();
                        virtual TPCE::CBaseLoader<TPCE::SETTLEMENT_ROW> *CreateSettlementLoader();
                        virtual TPCE::CBaseLoader<TPCE::HOLDING_ROW> *CreateHoldingLoader();
                        virtual TPCE::CBaseLoader<TPCE::HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader();
                        virtual TPCE::CBaseLoader<TPCE::HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader();
                        virtual TPCE::CBaseLoader<TPCE::EXCHANGE_ROW> *CreateExchangeLoader();
                        virtual TPCE::CBaseLoader<TPCE::TRADE_TYPE_ROW> *CreateTradeTypeLoader();
                        virtual TPCE::CBaseLoader<TPCE::ACCOUNT_PERMISSION_ROW> *CreateAccountPermissionLoader();
                        virtual TPCE::CBaseLoader<TPCE::BROKER_ROW> *CreateBrokerLoader();
                        virtual TPCE::CBaseLoader<TPCE::CASH_TRANSACTION_ROW> *CreateCashTransactionLoader();
                        virtual TPCE::CBaseLoader<TPCE::CHARGE_ROW> *CreateChargeLoader();
                        virtual TPCE::CBaseLoader<TPCE::CUSTOMER_ROW> *CreateCustomerLoader();
                        virtual TPCE::CBaseLoader<TPCE::COMPANY_ROW> *CreateCompanyLoader();
                        virtual TPCE::CBaseLoader<TPCE::LAST_TRADE_ROW> *CreateLastTradeLoader();
                        virtual TPCE::CBaseLoader<TPCE::CUSTOMER_TAXRATE_ROW> *CreateCustomerTaxrateLoader();
                        virtual TPCE::CBaseLoader<TPCE::TAXRATE_ROW> *CreateTaxrateLoader();
                        virtual TPCE::CBaseLoader<TPCE::COMMISSION_RATE_ROW> *CreateCommissionRateLoader();
                        virtual TPCE::CBaseLoader<TPCE::TRADE_REQUEST_ROW> *CreateTradeRequestLoader();
                        virtual TPCE::CBaseLoader<TPCE::ZIP_CODE_ROW> *CreateZipCodeLoader();
                        virtual TPCE::CBaseLoader<TPCE::COMPANY_COMPETITOR_ROW> *CreateCompanyCompetitorLoader();
                        virtual TPCE::CBaseLoader<TPCE::DAILY_MARKET_ROW> *CreateDailyMarketLoader();
                        virtual TPCE::CBaseLoader<TPCE::FINANCIAL_ROW> *CreateFinancialLoader();
                        virtual TPCE::CBaseLoader<TPCE::INDUSTRY_ROW> *CreateIndustryLoader();
                        virtual TPCE::CBaseLoader<TPCE::NEWS_ITEM_ROW> *CreateNewsItemLoader();
                        virtual TPCE::CBaseLoader<TPCE::NEWS_XREF_ROW> *CreateNewsXRefLoader();
                        virtual TPCE::CBaseLoader<TPCE::ADDRESS_ROW> *CreateAddressLoader();
                        virtual TPCE::CBaseLoader<TPCE::STATUS_TYPE_ROW> *CreateStatusTypeLoader();
                        virtual TPCE::CBaseLoader<TPCE::SECTOR_ROW> *CreateSectorLoader();
                        virtual TPCE::CBaseLoader<TPCE::WATCH_ITEM_ROW> *CreateWatchItemLoader();
                        virtual TPCE::CBaseLoader<TPCE::WATCH_LIST_ROW> *CreateWatchListLoader();


                  private:
                        TPCE::CBaseLoader<TPCE::TRADE_TYPE_ROW > *ttl_;
                        TPCE::CBaseLoader<TPCE::CHARGE_ROW> *chl_;
                        TPCE::CBaseLoader<TPCE::EXCHANGE_ROW> *exl_;
                        TPCE::CBaseLoader<TPCE::SECURITY_ROW>   *sel_;

                        TPCE::CBaseLoader<TPCE::HOLDING_ROW> *hol_;
                        TPCE::CBaseLoader<TPCE::HOLDING_HISTORY_ROW> *hohl_;
                        TPCE::CBaseLoader<TPCE::HOLDING_SUMMARY_ROW> *hosl_;
                        TPCE::CBaseLoader<TPCE::BROKER_ROW> *brl_;
                        TPCE::CBaseLoader<TPCE::SETTLEMENT_ROW> *setl_;
                        TPCE::CBaseLoader<TPCE::CASH_TRANSACTION_ROW> *ctl_;
                        TPCE::CBaseLoader<TPCE::TRADE_HISTORY_ROW> *thl_;
                        TPCE::CBaseLoader<TPCE::TRADE_ROW>      *tl_;

                        TPCE::CBaseLoader<TPCE::CUSTOMER_ACCOUNT_ROW> *cal_;
                        TPCE::CBaseLoader<TPCE::ACCOUNT_PERMISSION_ROW> *apl_;

                        TPCE::CBaseLoader<TPCE::CUSTOMER_ROW> *cl_;
                        TPCE::CBaseLoader<TPCE::COMPANY_ROW>  *col_;

                        TPCE::CBaseLoader<TPCE::LAST_TRADE_ROW> *ltl_;
                        TPCE::CBaseLoader<TPCE::CUSTOMER_TAXRATE_ROW> *cutrl_;
                        TPCE::CBaseLoader<TPCE::TAXRATE_ROW> *taxl_;
                        TPCE::CBaseLoader<TPCE::COMMISSION_RATE_ROW> *corl_;
                        TPCE::CBaseLoader<TPCE::TRADE_REQUEST_ROW> *trl_;

                        TPCE::CBaseLoader<TPCE::ZIP_CODE_ROW> *zcl_;
                        TPCE::CBaseLoader<TPCE::COMPANY_COMPETITOR_ROW> *ccl_;
                        TPCE::CBaseLoader<TPCE::DAILY_MARKET_ROW> *dml_;
                        TPCE::CBaseLoader<TPCE::FINANCIAL_ROW> *finl_;
                        TPCE::CBaseLoader<TPCE::INDUSTRY_ROW>  *inl_;
                        TPCE::CBaseLoader<TPCE::NEWS_ITEM_ROW> *nil_;
                        TPCE::CBaseLoader<TPCE::NEWS_XREF_ROW> *nxrl_;
                        TPCE::CBaseLoader<TPCE::ADDRESS_ROW >   *addl_;
                        TPCE::CBaseLoader<TPCE::STATUS_TYPE_ROW> *stl_;
                        TPCE::CBaseLoader<TPCE::SECTOR_ROW> *scl_;
                        TPCE::CBaseLoader<TPCE::WATCH_ITEM_ROW> *wil_;
                        TPCE::CBaseLoader<TPCE::WATCH_LIST_ROW> *wll_;
                  };
                  /* end namespace */
            };

      };
};

#endif
