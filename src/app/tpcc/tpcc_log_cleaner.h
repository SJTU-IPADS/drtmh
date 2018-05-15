#ifndef NOCC_OLTP_TPCC_LOG_CLEANER_H
#define NOCC_OLTP_TPCC_LOG_CLEANER_H

#include "tpcc_worker.h"
#include "tpcc_mixin.h"
#include "tpcc_schema.h"
#include "framework/log_cleaner.h"

#include "util/rtm.h"

extern size_t nthreads;
extern size_t current_partition;

#define WARE_NOT_FOUND    -1
#define OUT_OF_DATE_ENTRY -2

namespace nocc {
  namespace oltp {
    namespace tpcc {

      class TpccLogCleaner : public LogCleaner {
      public:
        TpccLogCleaner():LogCleaner() {
          // for(uint w = 0;w < NumWarehouses();++w) {
          //   for(uint d = 0;d < NumDistrictsPerWarehouse();++d) {
          //     last_no_o_ids_[w][d] = 3000; // 3000 is magic number, which is the init num of new order
          //   }
          // }
        }

        // uint64_t last_no_o_ids_[240][10];
        virtual int clean_log(int table_id, uint64_t key, uint64_t seq, char *val,int length){
          
          int w_id;
          bool need_check_vlen = true;

          switch(table_id){
          case STOC:
            w_id = stockKeyToWare(key);
            break;
          case DIST:
            w_id = districtKeyToWare(key);
            break;
          case WARE:
            w_id = key;
            break;
          case NEWO:
            w_id = newOrderKeyToWare(key);
            if(length == 0){
              need_check_vlen = false;
            }
            break;
          case CUST:
            w_id = customerKeyToWare(key);
            break;
          case ORLI:
            w_id = orderLineKeyToWare(key);
            break;
          case ORDE:
            w_id = orderKeyToWare(key);
            break;
          case CUST_INDEX:
          case ORDER_INDEX:
          case HIST:
            return 0;
          default:
            // if(table_id >= 0 && table_id <= 10)return 0;
            fprintf(stdout,"recv tab %d key %lu seq %lu\n",table_id,key,seq);
            assert(false);
          }

          int p_id = WarehouseToPartition(w_id);
          assert(p_id >= 0 && p_id < 16);
          if(backup_stores_.find(p_id) == backup_stores_.end()){
            // printf("log_cleaner: %d\n", p_id);
            return WARE_NOT_FOUND;
          }
          MemDB* db = backup_stores_[p_id];
          
          int vlen = db->_schemas[table_id].vlen;          
          if(need_check_vlen) assert(vlen == length);
          
          // printf("table:%d, key: %lu, partition:%d \n",table_id, key, p_id );
          MemNode *node = db->stores_[table_id]->GetWithInsert(key);
          
          

          switch(table_id){
          case ORLI:
            {
              //DZY:: is this code logically right without the assertion?
              // assert(node->seq == 0 && node->value == NULL);
              char *buf = (char *)malloc(length);
              memcpy(buf,val,length);
              node->seq = seq;
              node->value = (uint64_t *)buf;
              return 0;
            }
          case NEWO:
            if(length == 0){
              int32_t no_o_id = static_cast<int32_t>(key << 32 >> 32);
              int32_t upper   = newOrderUpper(key);

              int32_t d_id = upper % 10;
              w_id = upper / 10;
              if(d_id == 0) {
                w_id -= 1;
                d_id = 10;
              }


              // lock
              uint64_t *&val_ptr = node->value;
              if(val_ptr != NULL)
                free(val_ptr);
              node->value = NULL;

              return 0;
            }

          }

          volatile uint64_t *lockptr = &(node->lock);
          while(unlikely((*lockptr != 0) ||
                 !__sync_bool_compare_and_swap(lockptr,0,1))){
            // printf("log looping!\n"); 
          }
          if(node->seq >= seq){
            *lockptr = 0;
            return OUT_OF_DATE_ENTRY;
          }
          uint64_t *&val_ptr = node->value;
          if(val_ptr == NULL){
            val_ptr = (uint64_t*)malloc(vlen);
          }
          memcpy((char*)val_ptr, val, vlen);
          
          node->seq = seq;
          
          *lockptr = 0;
          return 0;
        }
      };

    }
  }
}


#endif
