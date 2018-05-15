#ifndef NOCC_FRAMEWORK_BANK_SCHEMA_H
#define NOCC_FRAMEWORK_BANK_SCHEMA_H

#include "framework/utils/encoder.h"
#include "framework/utils/inline_str.h"
#include "framework/utils/macros.h"

#define ACCT 0
#define SAV  1
#define CHECK 2

#define MIN_BALANCE 10000
#define MAX_BALANCE 50000


//#define BANK_NUM_ACCOUNTS 100000
//define BANK_NUM_ACCOUNTS
// 25% perct of the accounts are hot
//#define BANK_NUM_HOT      4000

//#ifdef BANK_NORMAL

//#define BANK_NUM_HOT      0
//#define BANK_NUM_HOT      25000
//#define BANK_HOT_PECT     0
//#else

//#define BANK_NUM_HOT      2000
//#define BANK_HOT_PECT     2
//#define BANK_NUM_HOT      1000
//#define BANK_HOT_PECT     1
//#endif

#define BANK_MIN_BALANCE 10000
#define BANK_MAX_BALANCE 50000

//#ifdef BANK_NORMAL
//#define BANK_TX_HOT     0
//#else
// This number is defined in the original small bank paper
//#define BANK_TX_HOT      90
//#endif
//#define BANK_TX_HOT 0


/*   table accounts   */

#define ACCOUNTS_KEY_FIELDS(x,y) \
  x(uint64_t,a_custid)

//
#define ACCOUNTS_VALUE_FIELDS(x,y)\
  x(inline_str_16<64>,a_name)
DO_STRUCT(account,ACCOUNTS_KEY_FIELDS,ACCOUNTS_VALUE_FIELDS)

/* ------- */

/*   table savings   */

#define SAVINGS_KEY_FIELDS(x,y) \
  x(uint64_t,s_cusitid)

#define SAVINGS_VALUE_FIELDS(x,y) \
  x(float,s_balance)
DO_STRUCT(savings,SAVINGS_KEY_FIELDS,SAVINGS_VALUE_FIELDS)

/* ------- */


/*   table checking   */

#define CHECKING_KEY_FIELDS(x,y) \
  x(uint64_t ,c_custid)

#define CHECKING_VALUE_FIELDS(x,y) \
  x(float,c_balance)
     DO_STRUCT(checking,CHECKING_KEY_FIELDS,CHECKING_VALUE_FIELDS)

/* ------- */

#endif
