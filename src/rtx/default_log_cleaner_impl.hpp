#pragma once

#include "tx_config.h"

#include "global_vars.h"

#include "core/rrpc.h"
#include "core/logging.h"

#include "log_cleaner.hpp"
#include "msg_format.hpp"

extern size_t current_partition;

namespace nocc {

namespace rtx {

// provide a default implementation of the log cleaner
class DefaultLogCleaner : public LogCleaner {
 public:
  DefaultLogCleaner(int num,RRpc *rpc) :
      LogCleaner(num),
      rpc_handler_(rpc)
  {

  }

  void register_callback(int id,RRpc *rpc) {
    rpc->register_callback(std::bind(&DefaultLogCleaner::clean_log,this,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3,
                                     std::placeholders::_4),id,true);
  }

  void clean_log(int nid,int cid,char *msg, void *) {

    RTX_ITER_ITEM(msg,sizeof(RtxWriteItem)) {
      auto item = (RtxWriteItem *)ttptr;
      ttptr += item->len;

      // check whether to log
      if(!global_view->is_backup(current_partition,item->pid))
        continue;

      assert(item->pid != current_partition);
      auto store = get_backed_store(item->pid);
      assert(store != NULL);

      MemNode *node = store->stores_[item->tableid]->GetWithInsert((uint64_t)(item->key));
      char *new_val;
      if(item->len == 0) {
        /* a delete case */
        new_val = NULL;
      }
      volatile uint64_t *lockptr = &(node->lock);
      while(unlikely((*lockptr != 0) ||
                     !__sync_bool_compare_and_swap(lockptr,0,1))){
      }
      node->lock = 0;
      uint64_t old_seq = node->seq;
      node->seq   = 1;
      asm volatile("" ::: "memory");
#if EM_FASST || INLINE_OVERWRITE
      memcpy(node->padding, (char *)item + sizeof(RtxWriteItem),item->len);
#else
      if(unlikely(node->value == NULL)) {
        node->value = (uint64_t *)malloc(item->len);
      }
      memcpy((char *)(node->value),
             (char *)item + sizeof(RtxWriteItem),item->len);
#endif
      asm volatile("" ::: "memory");
      node->seq = old_seq + 2;
      asm volatile("" ::: "memory");
      node->lock = 0;
    } // end iterating
#if !PA
    char *reply_msg = rpc_handler_->get_reply_buf();
    rpc_handler_->send_reply(reply_msg,sizeof(uint64_t),nid,cid);
#endif
  }
 private:
  RRpc *rpc_handler_;
};

}; // namespace rtx
}; // namespace nocc
