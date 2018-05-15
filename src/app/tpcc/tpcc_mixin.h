#ifndef NOCC_FRAMEWORK_TPCC_MIXIN_H
#define NOCC_FRAMEWORK_TPCC_MIXIN_H

#include "memstore/memdb.h"
#include "framework/utils/macros.h"
#include "framework/utils/util.h"

#include <string>
#include <vector>

using namespace nocc::util;

#define MASK_UPPER ( (0xffffffffLL) << 32)

namespace nocc {
  namespace oltp {
    namespace tpcc {

      extern int g_uniform_item_dist;

      /* TPCC constants */
      constexpr inline ALWAYS_INLINE size_t NumItems() {  return 100000; }
      constexpr inline ALWAYS_INLINE size_t NumCustomersPerDistrict() {  return 3000; }
      constexpr inline ALWAYS_INLINE size_t NumDistrictsPerWarehouse() {  return 10; }

      inline ALWAYS_INLINE int32_t districtKeyToWare(int64_t d_key) {
        int did = d_key % 10;
        if(did == 0) {
          return (d_key / 10) - 1;
        }
        return d_key / 10;
      }

      inline ALWAYS_INLINE int32_t customerKeyToWare(int64_t c_key) {
        int32_t upper = (int32_t)((MASK_UPPER & c_key) >> 32);
        int did = (upper % 10);
        if(did == 0)
          return (upper / 10) - 1;
        return upper / 10;
      }

      inline ALWAYS_INLINE int32_t stockKeyToWare(int64_t s_key) {
        int sid = s_key % 100000;
        if (sid == 0)
          return s_key / 100000 - 1;
        return s_key / 100000;
      }

      inline ALWAYS_INLINE int32_t orderKeyToWare(int64_t o_key) {
        return customerKeyToWare(o_key);
      }

      inline ALWAYS_INLINE int32_t orderLineKeyToWare(int64_t ol_key) {
        int64_t oid = ol_key / 15;
        int32_t upper = oid / 10000000;
        int did = upper % 10;
        if(did == 0)
          return (upper / 10) - 1;
        return upper / 10;
          

        return customerKeyToWare(ol_key);
        //  int64_t upper = ol_key / 10000000
      }

      inline ALWAYS_INLINE int32_t newOrderKeyToWare(int64_t no_key) {
        return customerKeyToWare(no_key);
      }

      inline ALWAYS_INLINE int32_t newOrderUpper(int64_t no_key) {
        int32_t upper = (int32_t)((MASK_UPPER & no_key) >> 32);
        return upper;
      }

      inline ALWAYS_INLINE int64_t makeDistrictKey(int32_t w_id, int32_t d_id) {
        int32_t did = d_id + (w_id * 10);
        int64_t id = static_cast<int64_t>(did);
        // assert(districtKeyToWare(id) == w_id);
        return id;
      }
      inline ALWAYS_INLINE int64_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
        int32_t upper_id = w_id * 10 + d_id;
        int64_t id =  static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(c_id);
        // assert(customerKeyToWare(id) == w_id);
        return id;
      }
      inline ALWAYS_INLINE int64_t makeHistoryKey(int32_t h_c_id,int32_t h_c_d_id,
                                                  int32_t h_c_w_id, int32_t h_d_id, int32_t h_w_id) {
        int32_t cid = (h_c_w_id * 10 + h_c_d_id) * 3000 + h_c_id;
        int32_t did = h_d_id + (h_w_id * 10);
        int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
        return id;
      }
      inline ALWAYS_INLINE int64_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
        int32_t upper_id = w_id * 10 + d_id;
        int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
        // assert(orderKeyToWare(id) == w_id);
        return id;
      }

      inline ALWAYS_INLINE int64_t makeOrderIndex(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
        int32_t upper_id = (w_id * 10 + d_id) * 3000 + c_id;
        int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
        return id;
      }

      inline ALWAYS_INLINE int64_t makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
        int32_t upper_id = w_id * 10 + d_id;
        int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
        int64_t olid = oid * 15 + number;
        int64_t id = static_cast<int64_t>(olid);
        // assert(orderLineKeyToWare(id) == w_id);
        return id;
      }

      inline ALWAYS_INLINE int64_t makeStockKey(int32_t w_id, int32_t s_id) {
        int32_t sid = s_id + (w_id * 100000);
        int64_t id = static_cast<int64_t>(sid);
        // assert(stockKeyToWare(id) == w_id);
        return id;
      }

      inline ALWAYS_INLINE int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
        int32_t upper_id = w_id * 10 + d_id;
        int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
        // assert(newOrderKeyToWare(id) == w_id);
        return id;
      }


      /* functions related to customer index
       * These functions are defined in tpcc_loader.cc
       */
      void convertString(char *newstring, const char *oldstring, int size);
      bool compareCustomerIndex(uint64_t key, uint64_t bound);
      uint64_t makeCustomerIndex(int32_t w_id, int32_t d_id, std::string s_last, std::string s_first);

      /* The shared data structure used in all TPCC related classes */
      class TpccMixin {

      public:
        MemDB *store_;
        TpccMixin(MemDB *store) {      store_ = store;      }
        /* Helper function used in TPCC */
        std::string NameTokens[10] =
          { std::string("BAR"),
            std::string("OUGHT"),
            std::string("ABLE"),
            std::string("PRI"),
            std::string("PRES"),
            std::string("ESE"),
            std::string("ANTI"),
            std::string("CALLY"),
            std::string("ATION"),
            std::string("EING"),
          };

        static const size_t CustomerLastNameMaxSize = 5 * 3;

        /* Followng pieces of codes mainly comes from Silo */
        inline ALWAYS_INLINE uint32_t  GetCurrentTimeMillis()
        {
          // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
          // for now, we just give each core an increasing number
          static __thread uint32_t tl_hack = 0;
          return ++tl_hack;
        }

        // utils for generating random #s and strings
        inline ALWAYS_INLINE int CheckBetweenInclusive(int v, int lower, int upper)
        {
          INVARIANT(v >= lower);
          INVARIANT(v <= upper);
          return v;
        }

        inline ALWAYS_INLINE int RandomNumber(fast_random &r, int min, int max)
        {
          return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
        }

        inline ALWAYS_INLINE int NonUniformRandom(fast_random &r, int A, int C, int min, int max)
        {
          return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
        }

        inline ALWAYS_INLINE int  GetItemId(fast_random &r)
        {
          return CheckBetweenInclusive(
                                       g_uniform_item_dist ?
                                       RandomNumber(r, 1, NumItems()) :
                                       NonUniformRandom(r, 8191, 7911, 1, NumItems()),
                                       1, NumItems());
        }

        inline ALWAYS_INLINE int  GetCustomerId(fast_random &r)
        {
          return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1,
                                                        NumCustomersPerDistrict()), 1, NumCustomersPerDistrict());
        }

        // pick a number between [start, end)
        inline ALWAYS_INLINE unsigned  PickWarehouseId(fast_random &r, unsigned start, unsigned end)
        {
          INVARIANT(start < end);
          const unsigned diff = end - start;
          if (diff == 1)
            return start;
          return (r.next() % diff) + start;
        }

        inline size_t  GetCustomerLastName(uint8_t *buf, fast_random &r, int num)
        {
          const std::string &s0 = NameTokens[num / 100];
          const std::string &s1 = NameTokens[(num / 10) % 10];
          const std::string &s2 = NameTokens[num % 10];
          uint8_t *const begin = buf;
          const size_t s0_sz = s0.size();
          const size_t s1_sz = s1.size();
          const size_t s2_sz = s2.size();
          NDB_MEMCPY(buf, s0.data(), s0_sz); buf += s0_sz;
          NDB_MEMCPY(buf, s1.data(), s1_sz); buf += s1_sz;
          NDB_MEMCPY(buf, s2.data(), s2_sz); buf += s2_sz;
          return buf - begin;
        }

        inline ALWAYS_INLINE size_t  GetCustomerLastName(char *buf, fast_random &r, int num)
        {
          return GetCustomerLastName((uint8_t *) buf, r, num);
        }

        inline std::string  GetCustomerLastName(fast_random &r, int num)
        {
          std::string ret;
          ret.resize(CustomerLastNameMaxSize);
          ret.resize(GetCustomerLastName((uint8_t *) &ret[0], r, num));
          return ret;
        }

        inline ALWAYS_INLINE std::string  GetNonUniformCustomerLastNameLoad(fast_random &r)
        {
          return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
        }

        inline ALWAYS_INLINE size_t   GetNonUniformCustomerLastNameRun(uint8_t *buf, fast_random &r)
        {
          return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
        }

        inline ALWAYS_INLINE size_t      GetNonUniformCustomerLastNameRun(char *buf, fast_random &r)
        {
          return GetNonUniformCustomerLastNameRun((uint8_t *) buf, r);
        }

        inline ALWAYS_INLINE std::string      GetNonUniformCustomerLastNameRun(fast_random &r)
        {
          return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
        }

        // following oltpbench, we really generate strings of len - 1...
        inline std::string      RandomStr(fast_random &r, uint len)
        {
          // this is a property of the oltpbench implementation...
          if (!len)
            return "";

          uint i = 0;
          std::string buf(len - 1, 0);
          while (i < (len - 1)) {
            const char c = (char) r.next_char();
            // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
            // is a less restrictive filter than isalnum()
            if (!isalnum(c))
              continue;
            buf[i++] = c;
          }
          return buf;
        }

        // RandomNStr() actually produces a string of length len
        inline std::string      RandomNStr(fast_random &r, uint len)
        {
          const char base = '0';
          std::string buf(len, 0);
          for (uint i = 0; i < len; i++)
            buf[i] = (char)(base + (r.next() % 10));
          return buf;
        }
      };

      class checker {
      public:
        static inline ALWAYS_INLINE void      SanityCheckCustomer(const customer::value *v) {
          INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
          INVARIANT(v->c_middle == "OE");
        }

        static inline ALWAYS_INLINE void      SanityCheckCustomer(const customer::key *k, const customer::value *v)
        {

          INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
          INVARIANT(v->c_middle == "OE");
        }

        static inline ALWAYS_INLINE void      SanityCheckWarehouse(const warehouse::key *k, const warehouse::value *v)
        {
          INVARIANT(v->w_state.size() == 2);
          INVARIANT(v->w_zip == "123456789");
        }

        static inline ALWAYS_INLINE void      SanityCheckDistrict (const district::value *v) {
          INVARIANT(v->d_next_o_id >= 3001 && v->d_next_o_id < 1000000);
          INVARIANT(v->d_state.size() == 2);
          INVARIANT(v->d_zip == "123456789");
        }

        static inline ALWAYS_INLINE void      SanityCheckDistrict(const district::key *k, const district::value *v)
        {
          INVARIANT(v->d_next_o_id >= 3001);
          INVARIANT(v->d_state.size() == 2);
          INVARIANT(v->d_zip == "123456789");
        }

        static inline ALWAYS_INLINE void      SanityCheckItem(const item::key *k, const item::value *v)
        {
          INVARIANT(v->i_price >= 1.0 && v->i_price <= 100.0);
        }

        static inline ALWAYS_INLINE void      SanityCheckStock(const stock::key *k, const stock::value *v)
        {
        }

        static inline ALWAYS_INLINE void      SanityCheckNewOrder(const new_order::key *k, const new_order::value *v)
        {
        }

        static inline ALWAYS_INLINE void      SanityCheckOOrder(const oorder::value *v)
        {
          INVARIANT(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
          INVARIANT(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
          INVARIANT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
        }

        static inline ALWAYS_INLINE void      SanityCheckOrderLine(const order_line::key *k, const order_line::value *v)
        {
          INVARIANT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
        }
      };
    }
  }
}
#endif
