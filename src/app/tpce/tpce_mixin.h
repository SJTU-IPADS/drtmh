#ifndef NOCC_OLTP_TPCE_MIXIN_H
#define NOCC_OLTP_TPCE_MIXIN_H

#include "memstore/memdb.h"
#include "framework/utils/macros.h"
#include "framework/utils/util.h"

#include "tpce_constants.h"

#include <string>
#include <vector>
#include <string.h>

using namespace nocc::util;

#define MIN(x,y) ((x) > (y)?(y):(x))

extern size_t current_partition;

namespace nocc {
  namespace oltp {
    namespace tpce {

      /* since the running environment is a distributed setting, thus trade_id shall
         mark which server it belongs.
      */
      uint64_t inline ALWAYS_INLINE encode_trade_id(uint64_t tid,int worker_id,int cid) {
        uint64_t header = (current_partition << 16) | (worker_id << 10) | (cid);
        uint64_t res = (header << 32) | (tid);
        return res;
      }
    
      int inline ALWAYS_INLINE
        decode_trade_mac(uint64_t tid) {
        uint64_t header = tid >> 32;
        return header >> 16;
      }

      int inline ALWAYS_INLINE
        decode_trade_worker(uint64_t tid) {
        uint64_t header = tid >> 32;
        uint64_t lower  = header & 0xffff;
        return lower >> 10;
      }

      int inline ALWAYS_INLINE
        decode_trade_routine(uint64_t tid) {
        return (tid >> 32) & 0x3ff;
      }

      uint64_t inline ALWAYS_INLINE
        decode_trade_payload(uint64_t tid) {
        uint64_t res = tid & 0xffffffff;
        return res;
      }
        
        void inline ALWAYS_INLINE
        print_trade_id(uint64_t tid) {
            fprintf(stdout,"tid %lu, worker %d, mac %d cid %d\n",
                    decode_trade_payload(tid),
                    decode_trade_worker(tid),
                    decode_trade_mac(tid),
                    decode_trade_routine(tid));
        }
    
      inline ALWAYS_INLINE
        int companyToPartition(uint64_t co_id) {
        return (co_id % 10000000 - 1) / companyPerPartition;
      }

      inline ALWAYS_INLINE
        int brokerToPartition(uint64_t bid) {
        return (bid % 10000000 - 1) / brokerPerPartition;
      }
    
      inline ALWAYS_INLINE
        int caToPartition(uint64_t acct_id) {
        uint64_t ca_id_norm = acct_id - 43000000000 - 1;
        return (ca_id_norm / caPerPartition);
      }
    
      inline ALWAYS_INLINE
        int64_t makeChargeKey(int tt_id,int ct) {
        return static_cast<int64_t> (tt_id) << 32 | static_cast<int64_t> (ct);
      }
    
      inline ALWAYS_INLINE
        uint64_t makeSecurityIndex(const char *k) {
        uint64_t *sec = new uint64_t[2];
        memset((char *)sec,0,sizeof(uint64_t) * 2);
        assert(strlen(k) < 2 * sizeof(uint64_t)); /* fixme, other places uses this setting */
        //      assert(strlen(k) < 3 * sizeof(uint64_t) );
        strcpy((char *)sec,k);
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeSecuritySecondIndex(uint64_t co_id, const char *issue) {
        uint64_t *sec = new uint64_t[5];
        memset((char *)sec,0,sizeof(uint64_t) * 5);
        sec[0] = co_id;
        memcpy(&(sec[1]),issue,strlen(issue));
        return (uint64_t )sec;
      }
    
      inline ALWAYS_INLINE
        uint64_t makeSecurityIndex(std::string &key) {
        return makeSecurityIndex(key.c_str());
      }
    
      inline ALWAYS_INLINE
        /* ca_id, trade_id, security symbol */
        uint64_t makeHoldingKey(uint64_t ca_id,uint64_t t_id,std::string &symb,uint64_t h_dts) {
        uint64_t *sec = new uint64_t[5];
        memset((char *)sec,0,sizeof(uint64_t) * 5);
        sec[0] = ca_id;
        sec[3] = t_id;
        sec[4] = h_dts;
        memcpy(&(sec[1]),symb.c_str(),symb.size());
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeHoldingHistKey(uint64_t t_id1,uint64_t t_id2) {
        uint64_t *sec = new uint64_t[2];
        sec[0] = t_id1;sec[1] = t_id2;
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeHoldingKey(uint64_t ca_id,uint64_t t_id,const char *symb,uint64_t h_dts) {
        uint64_t *sec = new uint64_t[5];
        memset((char *)sec,0,sizeof(uint64_t) * 5);
        sec[0] = ca_id;
        sec[3] = t_id;
        sec[4] = h_dts;
        strcpy((char *)( &sec[1]),symb);
        //      asm volatile("" ::: "memory");
        //      assert(sec[4] == t_id);
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        bool compareHoldingKey(uint64_t a,uint64_t bound) {
        uint64_t *k = (uint64_t *)a;
        uint64_t *b = (uint64_t *)bound;
        for (int i=0; i < 5; i++) {
          if (k[i] > b[i]) return false;
          if (k[i] < b[i]) return true;
        }
        return true;
      }

      inline ALWAYS_INLINE
        bool compareSecurityKey(uint64_t a,uint64_t b) {
        return strcmp((char *)a,(char *)b) == 0;
      }
          
      inline ALWAYS_INLINE
        uint64_t makeHSKey(uint64_t ca_id, std::string &symb) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
        sec[0] = ca_id;
        memcpy(&(sec[1]),symb.c_str(),symb.size());
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeCCKey(uint64_t co_id,uint64_t co_c_id,const char *industry) {
        uint64_t *sec = new uint64_t[3];
        sec[0] = co_id;
        sec[1] = co_c_id;
        sec[2] = *((int16_t *)(industry));
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeHSKey(uint64_t ca_id, const char *symb) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
        sec[0] = ca_id;
        strcpy((char *)(&(sec[1])),symb);
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE /* trade history key */
        uint64_t makeTHKey(uint64_t t_id, uint64_t dts,const char *status) {
        uint64_t *sec = new uint64_t[3];
        sec[0] = t_id;
        sec[1] = dts;
        sec[2] = 0;
        strcpy((char *)(&(sec[2])),status);
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE /* trade request key */
        uint64_t makeTRKey(uint64_t tid, uint64_t bid,const char *symb,const char *sec_id) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
        strcpy((char *)(&(sec[4])),sec_id);
        sec[2] = bid;
        sec[3] = tid;
        strcpy((char *)(&sec[0]), symb);
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeAPKey(uint64_t ca_id,std::string tax_id) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
      
        sec[0] = ca_id;
        memcpy(&(sec[1]),tax_id.c_str(),tax_id.size());
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t  makeAPKey(uint64_t ca_id,const char *tax_id,int len) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
      
        sec[0] = ca_id;
        memcpy(&(sec[1]),tax_id,len);
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeCOName(std::string name) {
        assert(name.size() < 48);
        uint64_t *sec = new uint64_t[6];
        memset(sec,0,sizeof(uint64_t) * 6);
        memcpy(sec,name.c_str(),MIN(name.size(),48));
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeCOName(char *name) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
        memcpy(sec,name,MIN(strlen(name),40));
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeCustTaxKey(uint64_t c_id, uint64_t tax_id) {
        return c_id << 32 | tax_id;
      }

      inline ALWAYS_INLINE
        uint64_t makeTaxCustKey(std::string tax_id,uint64_t c_id) {
        uint64_t *sec = new uint64_t[3];
        memset(sec,0,sizeof(uint64_t) * 3);
        sec[2] = c_id;
        memcpy(&(sec[0]),tax_id.data(),tax_id.size());
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeCustAcctKey(uint64_t cust,uint64_t ca) {
        uint64_t *sec = new uint64_t[2];
        sec[0] = cust;
        sec[1] = ca;
        return (uint64_t )sec;
      }

      inline ALWAYS_INLINE /* Commission rate */
        uint64_t makeCRKey(uint64_t c_tie, const char *tt_id,const char *ex_id,uint64_t qty) {
        assert(strlen(ex_id) < 8);
        uint64_t *sec = new uint64_t[4];
        sec[0] = c_tie;
        sec[1] = 0; strcpy((char *)(&sec[1]),tt_id);
        sec[2] = 0; strcpy((char *)(&sec[2]),ex_id);
        sec[3] = qty;
        return (uint64_t )sec ;
      }

      inline ALWAYS_INLINE
        uint64_t makeDMKey(const char *symb,uint64_t dm) {
        uint64_t *sec = new uint64_t[4];
        memset(sec,0,sizeof(uint64_t) * 4);
        strcpy((char *)(&(sec[0])),symb);
        sec[3] = dm;
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeFinKey(uint64_t co_id,uint64_t year,uint64_t qty) {
        uint64_t *sec = new uint64_t[3];
        sec[0] = co_id;
        sec[1] = year;
        sec[2] = qty;
        return (uint64_t )sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeNXRKey(uint64_t co_id,uint64_t n_id) {
        uint64_t *sec = new uint64_t[2];
        sec[0] = co_id;
        sec[1] = n_id;
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeSectorKey(const char *name,const char *id) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
        strcpy((char *)(&(sec[0])),name);
        strcpy((char *)(&(sec[4])),id);
        return (uint64_t )sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeSecondSecCompany(const char *sc_id,uint64_t co_id) {
        uint64_t *sec = new uint64_t[2];
        sec[0] = 0;
        strcpy( (char *)(&(sec[0])),sc_id);
        sec[1] = co_id;
        return (uint64_t)sec;
      }
      inline ALWAYS_INLINE
        uint64_t makeSecondCATrade(uint64_t ca_id,uint64_t dts,uint64_t tid) {
        uint64_t *sec = new uint64_t[3];
        sec[0] = ca_id;
        sec[1] = dts;
        sec[2] = tid;
        return (uint64_t) sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeSecondSecTrade(const char *symb, uint64_t dts, uint64_t t_id) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,5 * sizeof(uint64_t));
        sec[3] = dts;
        sec[4] = t_id;
        strcpy((char *)(&(sec[0])),symb);
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeSecondCompanyIndustrySecurity(uint64_t in_id,uint64_t co_id,const char *sec_key) {
        uint64_t *sec = new uint64_t[4];
        memset(&(sec[2]),0,sizeof(uint64_t) * 2);
        sec[0] = in_id;
        sec[1] = co_id;
        strcpy((char *)(&(sec[2])),sec_key);
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t makeSecondTradeRequest(const char *sc_id,uint64_t bid,const char *security, uint64_t tid) {
        uint64_t *sec = new uint64_t[5];
        memset(sec,0,sizeof(uint64_t) * 5);
        strcpy((char *)(&(sec[0])),sc_id);
        sec[1] = bid;
        strcpy((char *)(&(sec[2])),security);
        sec[4] = tid;
        return (uint64_t)sec;
      }

      inline ALWAYS_INLINE
        uint64_t transferToTRSec(uint64_t tr_key) {
        uint64_t *n_key = new uint64_t[5];
        uint64_t *o_key = (uint64_t *)tr_key;
        n_key[0] = o_key[4];
        n_key[1] = o_key[2];
        n_key[2] = o_key[0];
        n_key[3] = o_key[1];
        n_key[4] = o_key[3];
        return (uint64_t )n_key;
      }
      inline ALWAYS_INLINE
        uint64_t transferToTR(uint64_t key) {
        uint64_t *n_key = new uint64_t[5];
        uint64_t *o_key = (uint64_t *)key;
        n_key[0] = o_key[2];
        n_key[1] = o_key[3];
        n_key[2] = o_key[1];
        n_key[3] = o_key[4];
        n_key[4] = o_key[0];
        return (uint64_t )n_key;
      }
   
      /* Some debug utilities */
      inline ALWAYS_INLINE
        void printSecondTRKey(uint64_t k) {
        uint64_t *key = (uint64_t *)k;
        fprintf(stdout,"Sec: %s, status %s, b_id %lu, trade_id %lu\n",
                (char *)(&(key[2])), (char *)(&(key[0])),key[1],key[4]);
     
      }

      inline ALWAYS_INLINE
        void printTRKey(uint64_t k) {
        uint64_t *key = (uint64_t *)k;
        fprintf(stdout,"TradeRequest key: Sec %s, status %s, bid %lu, trade id %lu\n",
                (char *)(&(key[0])), (char *)(&(key[4])),key[2],key[3]);
      }

      inline ALWAYS_INLINE
        void printTradeHistKey(uint64_t k) {
        uint64_t *key = (uint64_t *)k;
        fprintf(stdout,"TradeHistory key: tid %d,%d,%d=%lu, time %lu, status: %s\n",
                decode_trade_mac(key[0]),decode_trade_worker(key[0]),
                decode_trade_routine(key[0]),decode_trade_payload(key[0]),
                key[1],(char *)(&(key[2])));
      }

      inline ALWAYS_INLINE
        void printTradeReqKey(uint64_t k) {
        uint64_t *key = (uint64_t *)k;
        fprintf(stdout,"Trade req key: tid %d,%d,%d=%lu, bid %lu, security: %s\n",
                decode_trade_mac(key[3]),decode_trade_worker(key[3]),
                decode_trade_routine(key[3]),decode_trade_payload(key[3]),
                key[2],(char *)(&(key[0])));
      }
   
   
      inline ALWAYS_INLINE
        void printHoldingKey(uint64_t k) {
        uint64_t *key = (uint64_t *)k;
        fprintf(stdout,"Holding key key ca id %lu, tid %lu, symb %s date %lu\n",
                key[0],key[3],(char *)(&(key[1])),key[4]);
      }

      inline ALWAYS_INLINE
        void printHSKey(uint64_t k) {
        uint64_t *key = (uint64_t *)k;
        fprintf(stdout,"Holding summary key %lu %s\n", key[0],(char *)( &(key[1])));
      }

      inline ALWAYS_INLINE
        void printSecondST(uint64_t k) {
        uint64_t *key = (uint64_t *)k;
        fprintf(stdout,"Second %s %lu tid: %lu\n",(char *)(&(key[0])),key[3],key[4]);
      }

   
      class TpceMixin  {
      public:
        MemDB *store_;
      TpceMixin(MemDB *store)
        :store_(store) {
      
        }
    
      };

  
    };
  };
};

#endif 




