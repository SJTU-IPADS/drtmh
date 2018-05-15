#ifndef NOCC_OLTP_BANK_LOG_CLEANER_H
#define NOCC_OLTP_BANK_LOG_CLEANER_H

#include "bank_worker.h"
#include "framework/log_cleaner.h"

extern size_t nthreads;
extern size_t current_partition;


namespace nocc {
  namespace oltp {
    namespace bank {

      class BankLogCleaner : public LogCleaner {
      public:
        BankLogCleaner():LogCleaner() {}

        virtual int clean_log(int table_id, uint64_t key, uint64_t seq, char *val,int length){
          
          int p_id = AcctToPid(key);
          assert(p_id >= 0 && p_id < 16);
          if(backup_stores_.find(p_id) == backup_stores_.end()){
            // printf("log_cleaner: %d\n", p_id);
            return -1;
          }
          MemDB* db = backup_stores_[p_id];
          
          int vlen = db->_schemas[table_id].vlen;          
          assert(vlen == length);
          // printf("table:%d, key: %lu, partition:%d \n",table_id, key, p_id );
          MemNode *node = db->stores_[table_id]->GetWithInsert(key);
          
          volatile uint64_t *lockptr = &(node->lock);
          while(unlikely((*lockptr != 0) ||
                 !__sync_bool_compare_and_swap(lockptr,0,1))){ }
          if(node->seq >= seq){
            *lockptr = 0;
            return -2;
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
