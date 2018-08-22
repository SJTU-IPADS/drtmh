#ifndef NOCC_OLTP_TPCE_H
#define NOCC_OLTP_TPCE_H

#include "framework/utils/encoder.h"
#include "framework/utils/inline_str.h"
#include "framework/utils/macros.h"

#define COMPRESS 1 // compress the database

/* Main table ids */
#define BROKER 0
#define CUST_ACCT 1 
#define TRADETYPE 2
#define CHARGE 3
#define EXCHANGE 4
#define SECURITY 5
#define TRADE    6
#define HOLDING  7
#define HOLDING_HIST 8
#define HOLDING_SUM  9
#define SETTLEMENT   10
#define CASH_TX      11
#define TRADE_HIST   12
#define ECUST     13  /* add an e-cust to distigunish from TPC-C */
#define CUSTACCT 14
#define ACCTPER  15
#define COMPANY  16
#define LT       17 /* last trade */
#define CUST_TAX 18
#define CR       19
#define TRADE_REQ 20
#define COMPANY_C 21
#define DAILY_MARKET 22
#define FINANCIAL    23
#define NEWS_XREF    24
#define SECTOR       25
#define LT1          26

 /* secondary indexs */
#define COMPANY_NAME 0
#define SEC_IDX 1
#define SEC_TAX_CUST  2
#define SEC_CA_TRADE  3
#define SEC_S_T       4  /* security related trade */
#define SEC_SC_CO     5
#define SEC_SC_TR     6
#define SEC_SC_INS    7

/* ---- Broker related tables ------ */

/* Broker table (broker: 经纪人*/
/*
  This group of tables contains data related to the brokerage firm and brokers.
 */
/*
  The table contains information about brokers.
 */
#define BROKER_KEY_FIELDS(x,y)			\
  x(int64_t, b_id)
#define BROKER_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<5>,b_st_id)		\
  y(inline_str_fixed<49>, b_name)		\
  y(int32_t, b_num_trades)			\
  y(double , b_comm_total)
DO_STRUCT(broker, BROKER_KEY_FIELDS, BROKER_VALUE_FIELDS)


/*
  COMMISSION_RATE
  The commission rate depends on several factors: the tier the customer is in, the type of trade, the
  quantity of securities traded, and the exchange that executes the trade.
*/

#define COMMISSION_RATE_KEY_FIELDS(x,y)\
  x(int32_t, cr_c_tier)\
  y(inline_str_fixed<3>, cr_tt_id )\
  y(inline_str_fixed<6>, cr_ex_id )\
  y(int32_t, cr_from_qty)
#define COMMISSION_RATE_VALUE_FIELDS(x,y)\
  x(int32_t, cr_to_qty)			 \
  y(double, cr_rate )
DO_STRUCT(commission_rate, COMMISSION_RATE_KEY_FIELDS, COMMISSION_RATE_VALUE_FIELDS )

/*
 * Charge table
 * Charge responsds to what is the price of each type of trade, each trade has multiple 
 * prices corresponds to multiple customer tie.
 * 
 * So, the key corresponds to 2 parts {type<string>}|{tie}
 * Luckily, type table is fixed, thus it is able to pre-analyzed a string->type mapping 
 * So, now it is {type<int>} | {tie<int>} 
 * and there is mapping such that type<string> => type<int>
 */

#define CHARGE_KEY_FIELDS(x, y)			\
  x(int64_t,ch_id)				
#define CHARGE_VALUE_FIELDS(x, y)		\
  x(double,ch_chrg)
DO_STRUCT(charge, CHARGE_KEY_FIELDS, CHARGE_VALUE_FIELDS)

/* The table contains a list of valid trade types. */
/* I replace the tt_id<str> with an id, a mapping between str->id must be maintained */
#define TRADE_TYPE_KEY_FIELDS(x,y)		\
  x(int64_t,tt_id)				
#define TRADE_TYPE_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<12>,tt_name)		\
  y(bool,  tt_is_sell)				\
  y(bool,   tt_is_mrkt)
DO_STRUCT(trade_type, TRADE_TYPE_KEY_FIELDS, TRADE_TYPE_VALUE_FIELDS)

/* Trade table */
/*
  The table contains information about trades.
 */

#define TRADE_KEY_FIELDS(x,y)			\
  x(int64_t, t_id )
#define TRADE_VALUE_FIELDS(x,y)			\
  x(uint64_t, t_dts )				\
  y(inline_str_fixed<5>, t_st_id)		\
  y(inline_str_fixed<5>, t_tt_id)		\
  y(inline_str_fixed<17>, t_s_symb)		\
  y(bool, t_is_cash )				\
  y(int32_t,t_qty)				\
  y(double,   t_bid_price)			\
  y(int64_t,    t_ca_id)			\
  y(inline_str_fixed<52>, t_exec_name)		\
  y(double,  t_trade_price)			\
  y(double,  t_chrg)				\
  y(double,  t_comm)				\
  y(double,  t_tax)				\
  y(bool,    t_lifo)
DO_STRUCT(trade, TRADE_KEY_FIELDS, TRADE_VALUE_FIELDS )

/* Trade history table */
/*
  The table contains the history of each trade transaction through the various states.
*/

#define TRADE_HISTORY_KEY_FIELDS(x,y)		\
  x(int64_t,    th_t_id)			\
  y(inline_str_fixed<4>, th_st_id)		\
  y(uint64_t,    th_dts)
#define TRADE_HISTORY_VALUE_FIELDS(x,y)		\
  x(bool, dummy)
DO_STRUCT(trade_history, TRADE_HISTORY_KEY_FIELDS, TRADE_HISTORY_VALUE_FIELDS )

/*
  TRADE_REQUEST
  The table contains information about pending limit trades that are waiting for a certain security price
  before the trades are submitted to the market.
 */
#define TRADE_REQUEST_KEY_FIELDS(x,y)\
  x(inline_str_fixed<15>, tr_s_symb)\
  y(uint64_t,    tr_b_id)\
  y(uint64_t,tr_t_id)
#define TRADE_REQUEST_VALUE_FIELDS(x,y)\
  x(inline_str_fixed<4>,tr_tt_id)\
  y(int32_t,     tr_qty)\
  y(double,   tr_bid_price)\
  y(inline_str_fixed<16>, dummy)
DO_STRUCT(trade_request, TRADE_REQUEST_KEY_FIELDS, TRADE_REQUEST_VALUE_FIELDS )

/* SEETLEMENT table 
   The table contains information about how trades are settled: specifically whether the settlement is on
   margin or in cash and when the settlement is due.
*/

#define SETTLEMENT_KEY_FIELDS(x,y)\
  x(int64_t, se_t_id )
#define SETTLEMENT_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<40>, se_cash_type)		\
  y(uint64_t, se_cash_due_date)			\
  y(double, se_amt )
DO_STRUCT(settlement, SETTLEMENT_KEY_FIELDS, SETTLEMENT_VALUE_FIELDS )

/*
  Cash transaction table
  The table contains information about cash transactions.
 */

#define CASH_TRANSACTION_KEY_FIELDS(x, y)\
  x(int64_t,ct_t_id)
#define CASH_TRANSACTION_VALUE_FIELDS(x, y)\
  x(uint64_t,ct_dts )				\
  y(float ,ct_amt )				\
  y(inline_str_fixed<102>,ct_name )
DO_STRUCT(cash_transaction, CASH_TRANSACTION_KEY_FIELDS, CASH_TRANSACTION_VALUE_FIELDS)

/*************************************************/


/* Customer tables  
   These groups of tables contain information about customer related data.
 */
     

#define CUSTOMERS_KEY_FIELDS(x,y)\
  x(int64_t,     c_id)
#define CUSTOMERS_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<20>,  c_tax_id)		\
  y(inline_str_fixed<5>,  c_st_id)		\
  y(inline_str_fixed<25>,  c_l_name)		\
  y(inline_str_fixed<20>,  c_f_name)		\
  y(inline_str_fixed<2>,  c_m_name)		\
  y(char,  c_gndr)				\
  y(char, c_tier)				\
  y(int32_t,     c_dob)				\
  y(int64_t,     c_ad_id)
/*
  y(inline_str_fixed<4>, c_ctry_1)\
  y(inline_str_fixed<4>, c_area_1)\
  y(inline_str_fixed<10>, c_local_1)\
  y(inline_str_fixed<6>, c_ext_1)\
  y(inline_str_fixed<4>, c_ctry_2)\
  y(inline_str_fixed<4>, c_area_2)		\
  y(inline_str_fixed<10>, c_local_2)\
  y(inline_str_fixed<6>, c_ext_2)\
  y(inline_str_fixed<5>, c_ctry_3)\
  y(inline_str_fixed<4>, c_area_3)\
  y(inline_str_fixed<10>, c_local_3)\
  y(inline_str_fixed<5>, c_ext_3)\
  y(inline_str_fixed<50>, c_email_1)\
  y(inline_str_fixed<50>, c_email_2)
  */
DO_STRUCT(customers, CUSTOMERS_KEY_FIELDS, CUSTOMERS_VALUE_FIELDS)

/*
  Account permission table. 
  This table contains information about the access the customer or an individual other than the customer has to a given customer account. Customer accounts may have trades executed on them by more than one person.
  ap_id =  ca_id | tax_id
 */

#define ACCOUNT_PERMISSION_KEY_FIELDS(x, y)\
  x(int64_t,    ap_id) 
#define ACCOUNT_PERMISSION_VALUE_FIELDS(x, y)\
  x(inline_str_fixed<5>, ap_acl)		\
  y(inline_str_fixed<25>, ap_l_name)		\
  y(inline_str_fixed<20>, ap_f_name)
DO_STRUCT(account_permission, ACCOUNT_PERMISSION_KEY_FIELDS, ACCOUNT_PERMISSION_VALUE_FIELDS)


/* customer account table 
   The table contains two references per customer into the TAXRATE table. One reference is for
state/province tax; the other one is for national tax. The TAXRATE table contains the actual tax rates.
 */

#define CUSTOMER_ACCOUNT_KEY_FIELDS(x,y)\
  x(int64_t,    ca_id)
#define CUSTOMER_ACCOUNT_VALUE_FIELDS(x,y)	\
  x(int64_t,    ca_b_id)			\
  y(int64_t,    ca_c_id)			\
  y(inline_str_fixed<50>,  ca_name )		\
  y(int16_t, ca_tax_st)				\
  y(double,   ca_bal)
DO_STRUCT(customer_account, CUSTOMER_ACCOUNT_KEY_FIELDS, CUSTOMER_ACCOUNT_VALUE_FIELDS)

/*
  CUSTOMER_TAXRATE
  The table contains two references per customer into the TAXRATE table. One reference is for
  state/province tax; the other one is for national tax. The TAXRATE table contains the actual tax rates.
*/

/* id contains cust_id | tax_id. B+ tree is needed */
#define CUSTOMER_TAXRATE_KEY_FIELDS(x,y)\
  x(int64_t,    id) 
#define CUSTOMER_TAXRATE_VALUE_FIELDS(x,y)\
  x(bool,    dummy)
DO_STRUCT(customer_taxrate, CUSTOMER_TAXRATE_KEY_FIELDS, CUSTOMER_TAXRATE_VALUE_FIELDS)

/* Holding 
   The table contains information about the customer account’s security holdings.The table contains information about the customer account’s security holdings.
   a holding id consists of  ca_id | symb_id | trade_id
*/
// TODO, miss dts in key 
#define HOLDING_KEY_FIELDS(x,y)\
  x(int64_t,    h_id) 
#define HOLDING_VALUE_FIELDS(x,y)\
  x(double,   h_price)				\
  y(int32_t,     h_qty)				\
  y(uint64_t ,   h_dts)				\
  y(inline_str_fixed<23>, dummy)
DO_STRUCT(holding, HOLDING_KEY_FIELDS, HOLDING_VALUE_FIELDS)


/*
  Holding history
  The table contains information about holding positions that were inserted, updated or deleted and
  which trades made each change.  
  Currenty i put the first trade here.
 */

#define HOLDING_HISTORY_KEY_FIELDS(x,y)\
  x(int64_t, hh_t_id)		       \
  y(int64_t, hh_h_t_id)
#define HOLDING_HISTORY_VALUE_FIELDS(x,y)	\
  x(int32_t,  hh_before_qty)			\
  y(int32_t,  hh_after_qty)
DO_STRUCT(holding_history, HOLDING_HISTORY_KEY_FIELDS, HOLDING_HISTORY_VALUE_FIELDS)

/*
  Holding summary
  The table contains aggregate information about the customer account’s security holdings.
 */

#define HOLDING_SUMMARY_KEY_FIELDS(x,y)		\
  x(int64_t,    hs_id)
#define HOLDING_SUMMARY_VALUE_FIELDS(x,y)\
  x(int32_t,     hs_qty)\
  y(inline_str_fixed<19>, dummy)
DO_STRUCT(holding_summary, HOLDING_SUMMARY_KEY_FIELDS, HOLDING_SUMMARY_VALUE_FIELDS)

/*
  WATCH_ITEM
  The table contains list of securities to watch for a watch list.
 */

#define WATCH_ITEM_KEY_FIELDS(x,y)		\
  x(int64_t,    wi_wl_id)			\
  y(inline_str_fixed<15>, wi_s_symb)
#define WATCH_ITEM_VALUE_FIELDS(x,y)		\
  x(bool, dummy)
DO_STRUCT(watch_item, WATCH_ITEM_KEY_FIELDS, WATCH_ITEM_VALUE_FIELDS)

/*
  WATCH_LIST
  The table contains information about the customer who created this watch list.
 */

#define WATCH_LIST_KEY_FIELDS(x,y)\
  x(int64_t, wl_c_id)\
  y(int64_t, wl_id)
#define WATCH_LIST_VALUE_FIELDS(x,y)\
  x(bool, dummy)
DO_STRUCT(watch_list, WATCH_LIST_KEY_FIELDS, WATCH_LIST_VALUE_FIELDS)


/*************************************************/

/* Market tables */
/* This group of tables contains information related to the exchanges, companies, and securities that create
   the Market. */

/*
  SECTOR
  The table contains information about market sectors.
 */

#define SECTOR_KEY_FIELDS(x,y)			\
  x(inline_str_fixed<30>, sc_name)		\
  y(inline_str_fixed<2>, sc_id)
#define SECTOR_VALUE_FIELDS(x,y)		\
  x(bool, dummy)
DO_STRUCT( sector, SECTOR_KEY_FIELDS, SECTOR_VALUE_FIELDS )

/* Company tables
   The table contains information about all companies with publicly traded securities.
 */
#define COMPANY_KEY_FIELDS(x,y)\
  x(int64_t,    co_id)
#define COMPANY_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<5>, co_st_id)		\
  y(inline_str_fixed<60>, co_name)		\
  y(inline_str_fixed<3>,co_in_id)		\
  y(inline_str_fixed<5>,co_sp_rate)		\
  y(int64_t,    co_ad_id)			\
  y(uint64_t,    co_open_date)   
  /*
    y(inline_str_fixed<150>, co_desc)		
    y(inline_str_fixed<46>,co_ceo)			*/
DO_STRUCT( company, COMPANY_KEY_FIELDS, COMPANY_VALUE_FIELDS )

/*
  DAILY_MARKET
  The table contains daily market statistics for each security, using the closing market data from the last completed trading day. EGenLoader will load this table with data for each security for the period starting 3 January 2000 and ending 31 December 2004.
*/

#define DAILY_MARKET_KEY_FIELDS(x,y)		\
  x(inline_str_fixed<15>, dm_s_symb)		\
  y(uint64_t,    dm_date)
#define DAILY_MARKET_VALUE_FIELDS(x,y)		\
  x(double,   dm_close)				\
  y(double,   dm_high)				\
  y(double,   dm_low)				\
  y(int64_t,     dm_vol)
DO_STRUCT( daily_market, DAILY_MARKET_KEY_FIELDS, DAILY_MARKET_VALUE_FIELDS )

/*
  COMPANY_COMPETITOR
  This table contains information for the competitors of a given company and the industry in which the
  company competes.  
*/
#define COMPANY_COMPETITOR_KEY_FIELDS(x,y)\
  x(int64_t,    cp_co_id)\
  y(int64_t,    cp_comp_co_id)\
  y(inline_str_fixed<2>, cp_in_id)
#define COMPANY_COMPETITOR_VALUE_FIELDS(x,y)\
  x(bool, dummy)
DO_STRUCT( company_competitor, COMPANY_COMPETITOR_KEY_FIELDS, COMPANY_COMPETITOR_VALUE_FIELDS )

/*
  LastTrade table
  The table contains one row for each security with the latest trade price and volume for each security.
 */
  //  x(inline_str_fixed<15>, lt_s_symb)
#define LAST_TRADE_KEY_FIELDS(x,y)\
  x(int64_t, lt_s_symb) 
#define LAST_TRADE_VALUE_FIELDS(x,y)\
  x(uint64_t,  lt_dts)\
  y(double,  lt_price)\
  y(double,  lt_open_price)\
  y(int64_t,  lt_vol)
DO_STRUCT( last_trade, LAST_TRADE_KEY_FIELDS, LAST_TRADE_VALUE_FIELDS )

/*
  FINANCIAL
  The table contains information about a company's quarterly financial reports. EGenLoader will load this table with financial information for each company for the Quarters starting 1 January 2000 and ending with the quarter that starts 1 October 2004.
*/

#define FINANCIAL_KEY_FIELDS(x,y)\
  x(int64_t,     fi_co_id)\
  y(int32_t,      fi_year)			\
  y(int32_t, fi_qtr)
#define FINANCIAL_VALUE_FIELDS(x,y)		\
  x(uint64_t,     fi_qtr_start_date)		\
  y(double,    fi_revenue)			\
  y(double,    fi_net_earn)			\
  y(double,    fi_basic_eps)			\
  y(double,    fi_dilut_eps)			\
  y(double,    fi_margin)			\
  y(double,    fi_inventory)			\
  y(double,   fi_assets)			\
  y(double,   fi_liability)			\
  y(int64_t,   fi_out_basic)			\
  y(int64_t,   fi_out_dilut)
DO_STRUCT( financial, FINANCIAL_KEY_FIELDS, FINANCIAL_VALUE_FIELDS )

/*
  INDUSTRY
  The table contains information about industries. Used to categorize which industries a company is in.
  // it is fixed.
*/
#define INDUSTRY_KEY_FIELDS(x,y)\
  x(inline_str_fixed<2>, in_id)
#define INDUSTRY_VALUE_FIELDS(x,y)\
  x(inline_str_fixed<50>, in_name)\
  y(inline_str_fixed<3>, in_sc_id)
DO_STRUCT( industry, INDUSTRY_KEY_FIELDS, INDUSTRY_VALUE_FIELDS )

/*
  NEWS_ITEM
  The table contains information about news items of interest.
*/
#if COMPRESS == 1
#define NEWS_ITEM_KEY_FIELDS(x,y)               \
  x(int64_t,    ni_id)
// FIXME. ni_item current not installed to save space
#define NEWS_ITEM_VALUE_FIELDS(x,y)             \
  x(uint64_t,  ni_dts)
DO_STRUCT( news_item, NEWS_ITEM_KEY_FIELDS, NEWS_ITEM_VALUE_FIELDS )

#else
#define NEWS_ITEM_KEY_FIELDS(x,y)		\
  x(int64_t,    ni_id)
// FIXME. ni_item current not installed to save space 
#define NEWS_ITEM_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<80>, ni_headline)		\
  y(inline_str_fixed<255>, ni_summary)	        \
  y(uint64_t,  ni_dts)				\
  y(inline_str_fixed<30>, ni_source)		\
  y(inline_str_fixed<30>, ni_author)
DO_STRUCT( news_item, NEWS_ITEM_KEY_FIELDS, NEWS_ITEM_VALUE_FIELDS )
#endif

/*
  NEWS_XREF
  The table contains a cross-reference of news items to companies that are mentioned in the news item.
*/

#define NEWS_XREF_KEY_FIELDS(x,y)		\
  x(int64_t,   nx_co_id)			\
  y(int64_t,   nx_ni_id)
#define NEWS_XREF_VALUE_FIELDS(x,y)		\
  x(bool,   dummy)
DO_STRUCT( news_xref, NEWS_XREF_KEY_FIELDS, NEWS_XREF_VALUE_FIELDS )

/*
   Excahange
   The table contains information about financial exchanges.
   as trade type, the id is transformed to an integer 
*/

#define EXCHANGE_KEY_FIELDS(x,y)		\
  x(int64_t, ex_id)
#define EXCHANGE_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<100>, ex_name)		\
  y(int32_t,     ex_num_symb)			\
  y(int32_t,     ex_open)			\
  y(int32_t,     ex_close)			\
  y(inline_str_fixed<150>, ex_desc)		\
  y(int64_t,    ex_ad_id)
DO_STRUCT( exchange, EXCHANGE_KEY_FIELDS, EXCHANGE_VALUE_FIELDS )


/* SECURITY table 
   This table contains information about each security traded on any of the exchanges.
   The symbol is convereted to a string.
 */

#define SECURITY_KEY_FIELDS(x,y)		\
  x(int64_t, s_symb)
#define SECURITY_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<7>, s_issue)		\
  y(inline_str_fixed<5>, s_st_id)		\
  y(inline_str_fixed<70>, s_name)		\
  y(inline_str_fixed<7>, s_ex_id)		\
  y(int64_t,   s_co_id)				\
  y(int64_t,   s_num_out)			\
  y(uint64_t,  s_start_date)			\
  y(uint64_t,   s_exch_date)			\
  y(double,s_pe)				\
  y(float, s_52wk_high)				\
  y(uint64_t,s_52wk_high_date)			\
  y(float,s_52wk_low)				\
  y(uint64_t,s_52wk_low_date)			\
  y(double,  s_dividend)			\
  y(double,  s_yield)
DO_STRUCT( security, SECURITY_KEY_FIELDS, SECURITY_VALUE_FIELDS )

/*************************************************/

/*
  Dimension Tables
  This group of tables includes 4 dimension tables that contain common information such as addresses
  and zip codes.  
*/

/*
  ADDRESS
  This table contains address information.
 */
#define ADDRESS_KEY_FIELDS(x,y)\
  x(int64_t,    ad_id)
#define ADDRESS_VALUE_FIELDS(x,y)\
  x(inline_str_fixed<80>, ad_line1)\
  y(inline_str_fixed<80>, ad_line2)\
  y(inline_str_fixed<12>, ad_zc_code)\
  y(inline_str_fixed<80>, ad_ctry)
DO_STRUCT( address, ADDRESS_KEY_FIELDS, ADDRESS_VALUE_FIELDS )

/*
  STATUS_TYPE
  This table contains all status values for several different status usages. Multiple tables reference this
  table to obtain their status values.
 */

#define STATUS_TYPE_KEY_FIELDS(x,y)\
  x(inline_str_fixed<4>, st_id)
#define STATUS_TYPE_VALUE_FIELDS(x,y)\
  x(inline_str_fixed<11>, st_name)
DO_STRUCT( status_type, STATUS_TYPE_KEY_FIELDS, STATUS_TYPE_VALUE_FIELDS )

/*
  ZIP_CODE
  The table contains zip and postal codes, towns, and divisions that go with them.
 */

#if COMPRESS == 1

#define ZIP_CODE_KEY_FIELDS(x,y)		\
  x(inline_str_fixed<12>, zc_code)
#define ZIP_CODE_VALUE_FIELDS(x,y)		\
  x(inline_str_fixed<1>, zc_town)
DO_STRUCT( zip_code, ZIP_CODE_KEY_FIELDS, ZIP_CODE_VALUE_FIELDS )

#else

#define ZIP_CODE_KEY_FIELDS(x,y)                \
  x(inline_str_fixed<12>, zc_code)
#define ZIP_CODE_VALUE_FIELDS(x,y)              \
  x(inline_str_fixed<80>, zc_town)              \
  y(inline_str_fixed<80>, zc_div)
DO_STRUCT( zip_code, ZIP_CODE_KEY_FIELDS, ZIP_CODE_VALUE_FIELDS )
#endif


#endif 


