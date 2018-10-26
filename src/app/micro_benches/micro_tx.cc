#include "all.h"
#include "tx_config.h"

#include "bench_micro.h"

#include "util/util.h"
#include "util/mapped_log.h"

#include "framework/req_buf_allocator.h"

#include "core/rrpc.h"

extern size_t distributed_ratio; // re-use some configure parameters
extern size_t nthreads;
extern size_t total_partition;

#include "../smallbank/bank_worker.h"
using namespace nocc::oltp::bank; // use smallbank as the default workload

extern __thread MappedLog local_log;

static inline void __lock_ts(volatile uint64_t *lock) {
  //  fprintf(stdout,"check read lock val %lu %p\n",*lock,lock);
  //  assert(__sync_bool_compare_and_swap(lock,0,1) == true);
  //  return;
  for(;;) {
    if( unlikely( ( (*lock) != 0) ||
                  !__sync_bool_compare_and_swap(lock,0,1) )
        ) {
      //      fprintf(stdout,"failed ,lock %lu\n",*lock);
      //      sleep(1);
      continue;
    } else
      return;
  }
}

static inline void __release_ts(volatile uint64_t *lock) {
  barrier();
  *lock = 0;
}

namespace nocc {

  extern RdmaCtrl *cm;

  namespace oltp {

    extern __thread util::fast_random   *random_generator;
    extern __thread RPCMemAllocator *msg_buf_alloctors;

    namespace micro {

      // arg communicate between RPCs
      struct MicroArg {
        uint64_t id;
        uint64_t version;
      };

      void MicroWorker::tx_write_ts(int id,int cid,char *msg,void *arg) {
        uint64_t key = *((uint64_t *)msg);
        uint64_t seq = *((uint64_t *)(msg + sizeof(uint64_t)));

        MemNode *node = store_->stores_[TAB]->GetWithInsert(key);
        __lock_ts(&(node->lock));
        node->seq = seq;
        __release_ts(&(node->lock));

        char *reply_buf = rpc_->get_reply_buf();
        rpc_->send_reply(reply_buf,sizeof(uint64_t),id,worker_id_,cid);
      }

      void MicroWorker::tx_ro_handler(int id,int cid,char *msg,void *arg) {

#if SI_TX
        // set a vector timestamp
        int ts_size = ts_manager->ts_size_;
#else
        int ts_size = sizeof(uint64_t);
#endif
#if 1
        uint64_t key = *((uint64_t *)(msg + ts_size));
        int pid = AcctToPid(key);
        MemNode *node = store_->stores_[TAB]->GetWithInsert(key);
        assert(node->value != NULL);

        char *reply_buf = rpc_->get_reply_buf();
        *((uint64_t *)reply_buf)  = key;
        *((uint64_t *)(reply_buf + sizeof(uint64_t))) = node->seq;

#if RAD_TX
        // set the lock
        __lock_ts(&(node->lock));
        node->seq = 73; // a dummy seq
        __release_ts(&(node->lock));
#endif
#endif
        rpc_->send_reply(reply_buf,sizeof(uint64_t) + sizeof(uint64_t),id,worker_id_,cid);
      }

      void MicroWorker::tx_read_handler(int id,int cid,char *msg,void *arg) {
#if 1
        uint64_t key = *((uint64_t *)msg);
        int pid = AcctToPid(key);
        MemNode *node = store_->stores_[TAB]->GetWithInsert(key);
        assert(node->value != NULL);

        char *reply_buf = rpc_->get_reply_buf();
        *((uint64_t *)reply_buf)  = key;
        *((uint64_t *)(reply_buf + sizeof(uint64_t))) = node->seq;
#endif
        rpc_->send_reply(reply_buf,sizeof(uint64_t) + sizeof(uint64_t),id,worker_id_,cid);
      }

      // 2 dummy handlers for test, they only send a dummy reply
      void MicroWorker::tx_one_shot_handler2(int id,int cid,char *msg,void *arg) {
        char *reply_msg = rpc_->get_reply_buf();
        rpc_->send_reply(reply_msg,CACHE_LINE_SZ,id,worker_id_,cid);
        return;
      }

      // This RPC is designed to yield out
      void MicroWorker::tx_one_shot_handler(yield_func_t &yield,int id,int cid,char *input) {
#if 0
        MicroArg *arg = (MicroArg *)input;
        char buf[CACHE_LINE_SZ];
        tx_->get_ro_versioned(TAB,arg->id,buf,arg->version,yield);
#endif
        char *reply_msg = rpc_->get_reply_buf();
        rpc_->send_reply(reply_msg,CACHE_LINE_SZ,id,worker_id_,cid);
        return;
      }

      txn_result_t MicroWorker::micro_tx_ts(yield_func_t &yield) {

        if(current_partition == 0) return txn_result_t(false,1);
        return txn_result_t(true,1);
      }

      bool local_account(uint64_t id) {
        static uint64_t num_accounts = NumAccounts() / total_partition;

        if(id >= num_accounts * current_partition && id < (current_partition + 1) * num_accounts)
          return true;
        return false;
      }

      inline int acct_to_pid(uint64_t id) {
        static uint64_t num_accounts = NumAccounts() / total_partition;
        return id / num_accounts;
      }

      inline uint64_t get_account(fast_random &rand, bool distributed) {

        static uint64_t num_accounts = NumAccounts() / total_partition;
        static uint64_t num_hot      = num_accounts * 0.25;
        assert(num_hot > 0 && num_accounts > 0);

        uint64_t nums_global;
        if(rand.next() % 100 < TX_HOT) {
          nums_global = num_hot;
        } else {
          nums_global = num_accounts;
        }
        uint64_t acct_id = rand.next() % nums_global;

        if(distributed) {
        retry:
          int id = rand.next() % total_partition;
          if(id == current_partition && total_partition != 1)
            goto retry;
          acct_id += (num_accounts * id);
        } else
          acct_id += num_accounts * current_partition;
        return acct_id;
      }

      txn_result_t MicroWorker::micro_tx_wait(yield_func_t &yield) {

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_read(yield_func_t &yield) {

        return micro_tx_wait(yield);
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_working_ts2(yield_func_t &yield) {

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_rad(yield_func_t &yield) {

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_rw(yield_func_t &yield) {

        return txn_result_t(true,1);
      }

    }; // namespace micro

  }; // namespace oltp

};   // namespace nocc
