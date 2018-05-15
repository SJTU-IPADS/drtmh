#include "all.h"
#include "bench_micro.h"

#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"

#include "util/util.h"
#include "util/mapped_log.h"

#include "framework/req_buf_allocator.h"

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
        tx_->begin();
        tx_->end(yield);
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

        static uint64_t total_accts[1000];
        int sleep_time = distributed_ratio;

        DBRad *tx = (DBRad *)tx_;

        if(worker_id_ == 0) { // writer
          for(uint i = 0;i < 10;++i) {



          }
        } else {

        }
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_read(yield_func_t &yield) {

        return micro_tx_wait(yield);
        static uint64_t num_accounts = NumAccounts() / total_partition;

        if(current_partition == 0) indirect_must_yield(yield);
      retry:
        char reply_buf[CACHE_LINE_SZ * 10];
        rpc_->prepare_multi_req(reply_buf,10,cor_id_);

        for(uint i = 0;i < 10;++i) {

          fast_random &rand = random_generator[cor_id_];
          uint64_t key = (rand.next() % total_partition) * num_accounts + rand.next() % num_accounts;

          int pid = acct_to_pid(key);

          char *req_buf = msg_buf_alloctors[cor_id_][i];

          int msg_size = sizeof(uint64_t);
          int ts_size  = sizeof(uint64_t);
#if SI_TX
        // set a vector timestamp
          DBSI *tx = (DBSI *)tx_;
          ts_size = ts_manager->ts_size_;
          *((uint64_t *)(req_buf + ts_size)) = key;
#else
          // set one scalar timestamp
          *((uint64_t *) req_buf) = 0; // a dummy timestamp
          *((uint64_t *) (req_buf + sizeof(uint64_t))) = key;
#endif
          msg_size += ts_size;
          rpc_->append_req(req_buf,RPC_READ_ONLY,msg_size,cor_id_,
                           RRpc::REQ,pid);
        }
        // send the requests
        indirect_yield(yield);
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_working_ts2(yield_func_t &yield) {

        if(current_partition == 0) indirect_must_yield(yield); // never execute logic at primary server

        //int working_set_num = distributed_ratio;
        int working_set_num = 1;
        //int read_ratio = distributed_ratio;
        int read_ratio = 1;
        char reply_buf[20 * sizeof(uint64_t) * 2];

        std::vector<uint64_t > local_records; // may duplicates, but not care
        std::vector<uint64_t > remote_records;
        std::map<uint64_t,MemNode *> write_set;
        std::map<uint64_t,MemNode *> read_set;

        fast_random &rand = random_generator[cor_id_];

        for(uint i = 0;i < working_set_num;++i) {
          if(total_partition != 1 && (rand.next() % 100) == 1) {
            // distributed case
            uint64_t key = get_account(rand,true);
            if(acct_to_pid(key) == 0) { i--;continue;}
            remote_records.push_back(key);
            char *req_buf = msg_buf_alloctors[cor_id_][i];
            *((uint64_t *)req_buf) = key;
            rpc_->append_req(req_buf,RPC_READ,sizeof(uint64_t),cor_id_,
                             RRpc::REQ,acct_to_pid(key));

            bool read = (rand.next() % 100) <= read_ratio;
            MemNode *dummy_node = NULL;
            if(read) read_set.insert(std::make_pair(key,dummy_node));
            else     write_set.insert(std::make_pair(key,dummy_node));
          } else {
            local_records.push_back(get_account(rand,false));
          }
        }

        rpc_->prepare_multi_req(reply_buf,remote_records.size(),cor_id_);

        std::set<int> vec_set; // used in vector clock to store the diff
        uint64_t tentative_ts = 0;

        for(uint i = 0;i < local_records.size();++i) {

          bool read = (rand.next() % 100) <= read_ratio;
          uint64_t key = local_records[i];

          MemNode *node = store_->stores_[TAB]->GetWithInsert(key);
          auto seq = node->seq;

          if(!read)  {
            write_set.insert(std::make_pair(key,node));
            uint s = SI_GET_SERVER(seq);
            if(vec_set.find(s) == vec_set.end()) vec_set.insert(s);
          } else {
            read_set.insert(std::make_pair(key,node));
          }
          tentative_ts = MAX(tentative_ts,seq);
        }

        if(remote_records.size() > 0)
          indirect_yield(yield);

        uint64_t *reply_seq = (uint64_t *)reply_buf;
        for(uint i = 0;i < remote_records.size();++i) {
          uint64_t seq = reply_seq[i];
          uint s = SI_GET_SERVER(seq);
          if(vec_set.find(s) == vec_set.end()) vec_set.insert(s);
        }

        // end commit phase
        uint64_t encoded_commit_ts;
#if SI_TX
        uint64_t commit_ts = ((DBSI *)tx_)->get_commit_ts(yield,encoded_commit_ts);
#elif RAD_TX
        //encoded_commit_ts = MAX(((DBRad *)tx_)->get_timestamp(),tentative_ts);
        encoded_commit_ts = 0;
#endif

        // write back
        int remote_write_num = 0;
        for(auto it = write_set.begin();it != write_set.end();++it) {

          auto key = it->first;
          auto node = it->second;
          if(acct_to_pid(key) == current_partition) {
            // local case
#ifndef OCC_TX
            __lock_ts(&(node->lock));
            node->seq = encoded_commit_ts;
            __release_ts(&(node->lock));
#endif
          } else {
            char *req_buf = msg_buf_alloctors[cor_id_][remote_write_num++];
            *((uint64_t *)req_buf) = key;
            *((uint64_t *)(req_buf + sizeof(uint64_t))) = encoded_commit_ts;
            rpc_->append_req(req_buf,RPC_READ + 1,sizeof(uint64_t) + sizeof(uint64_t),
                             cor_id_,RRpc::REQ,acct_to_pid(key));
          }
        }
#if RAD_TX
        for(auto it = read_set.begin();it != read_set.end();++it) {
          auto key = it->first;
          auto node = it->second;
          if(acct_to_pid(key) == current_partition) {
            // local case
            __lock_ts(&(node->lock));
            node->seq = encoded_commit_ts;
            __release_ts(&(node->lock));
          } else {
            char *req_buf = msg_buf_alloctors[cor_id_][remote_write_num++];
            *((uint64_t *)req_buf) = key;
            *((uint64_t *)(req_buf + sizeof(uint64_t))) = encoded_commit_ts;
            rpc_->append_req(req_buf,RPC_READ + 1,sizeof(uint64_t) + sizeof(uint64_t),
                             cor_id_,RRpc::REQ,acct_to_pid(key));
          }
        }
#endif

        rpc_->prepare_multi_req(reply_buf,remote_write_num,cor_id_);
        if(remote_write_num > 0)
          indirect_yield(yield);

        // SI need to write timestamp back
#if SI_TX == 1
        char *ts_buffer =  msg_buf_alloctors[cor_id_].get_req_buf();

        static __thread uint64_t sum = 0;
        static __thread int      count = 0;
#if SI_TX
        uint64_t vec_size = sizeof(uint64_t) * vec_set.size();
#if ONE_CLOCK
        vec_size = sizeof(uint64_t);
#endif
#endif
        int master_id = 0;
#if 0
        sum += vec_size;
        count += 1;
        if(count > 1000000) {
          fprintf(stdout,"avg tv size %f\n",(double)sum / (double)count);
          sum = 0;count = 0;
        }
#endif
        rpc_->append_req(ts_buffer + RRpc::rpc_padding(),
                         RPC_COMMIT + 1, vec_size, cor_id_,RRpc::REQ,master_id);
#endif
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_rad(yield_func_t &yield) {

        // PDI related micro benchmarks
        auto num = distributed_ratio; // number of item read
        tx_->begin();

        static const int num_nodes = cm->get_num_nodes();
        int pid = random_generator[cor_id_].next() % num_nodes;
        char *req_buf   = msg_buf_alloctors[cor_id_].get_req_buf();
        char *reply_buf = (char *)malloc(CACHE_LINE_SZ);

        int type = RRpc::REQ;
#ifdef RAD_TX
        type = RRpc::Y_REQ;
#endif
        rpc_->prepare_multi_req(reply_buf,1,cor_id_);
        rpc_->append_req(req_buf,RPC_READ,CACHE_LINE_SZ,cor_id_,type,pid);
        indirect_yield(yield);
        free(reply_buf);
        tx_->end(yield);
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_rw(yield_func_t &yield) {

        // test the basic performance of read/write using PDI
        int read_ratio = distributed_ratio;
        set<uint64_t> id_set;
        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();
        MicroArg *arg = (MicroArg *)req_buf;
        char *reply_buf = (char *)malloc(CACHE_LINE_SZ);

        tx_->begin();
        arg->version = ((DBRad *)tx_)->timestamp;
#if 1
        for(uint i = 0;i < 20;++i) {
          uint64_t id;
        retry:
          GetAccount(random_generator[cor_id_],&id);
          if(id_set.find(id) != id_set.end()) goto retry;
          else id_set.insert(id);

          int pid = AcctToPid(id);

          bool ro = random_generator[cor_id_].next() % 100 <= read_ratio;
          int idx = 0;
          if(ro) {
            arg->id = id;
            rpc_->prepare_multi_req(reply_buf,1,cor_id_);
            rpc_->append_req(req_buf,RPC_READ,sizeof(MicroArg),cor_id_,RRpc::REQ,pid);
            indirect_yield(yield);
          } else {
            //fprintf(stdout,"add to %d, key %lu\n",pid,id);
            idx = tx_->add_to_remote_set(TAB,id,pid);
            assert(idx == 0);
            auto replies = tx_->remoteset->do_reads(i);
            indirect_yield(yield);
            tx_->get_remote_results(replies);

            char *buf = NULL;
            uint64_t seq = tx_->get_cached(0,(char **)(&buf));
            tx_->remote_write(idx,buf,CACHE_LINE_SZ);
          }
        }
#endif
        bool ret = tx_->end(yield);
        free(reply_buf);
        return txn_result_t(ret,1);
      }

    }; // namespace micro

  }; // namespace oltp

};   // namespace nocc
