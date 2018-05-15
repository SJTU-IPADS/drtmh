#include "global_config.h"
#include "db/config.h"

#include "db_farm.h"

#include "framework/bench_worker.hpp"
#include "framework/req_buf_allocator.h"

// This file contains RPC sending and receiving of FaRM style processing

extern size_t current_partition; // current partition-id

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

#define META_LENGTH 16 // FIXME!! duplicate with the one in db_farm.cc

namespace nocc {

  extern __thread oltp::BenchWorker* worker;

  namespace oltp {
    extern __thread oltp::RPCMemAllocator *msg_buf_alloctors;
  }
  using namespace oltp;

  namespace db {

    // RPC sending mechanisms //////////////////////////////////////////////////

    struct RemoteCommitItem {
      uint8_t  pid;
      uint64_t off;
      uint8_t  payload;
    } __attribute__ ((aligned (8)));

    struct RemoteLockItem {
      uint8_t  pid;
      uint64_t off;
      uint64_t seq;
    } __attribute__ ((aligned (8)));

    struct LockReplyHeader {
      uint8_t res;
    } __attribute__ ((aligned (8)));

    void DBFarm::remote_write(int r_idx,char *val,int len) {

    RRWSet::Meta *m = (RRWSet::Meta *)(rrwset_->kvs_[r_idx].val); // parse the meta

#if USE_LOGGER
      if(db_logger_){
        // printf("remote_write size: %d\n", len);
        RRWSet::RemoteSetItem& item = rrwset_->kvs_[r_idx];
        char* logger_val = db_logger_->get_log_entry(cor_id_, item.tableid, item.key, len, item.pid);
        memcpy(logger_val, val, len);
        db_logger_->close_entry(cor_id_, m->seq + 2);
      }
#endif
      rrwset_->promote_to_write(r_idx);

      write_items_ += 1;

      // set the commit RPC req fields
      volatile RemoteCommitItem *pl = (RemoteCommitItem *)commit_buf_end_;
      pl->pid  = rrwset_->kvs_[r_idx].pid;
      pl->off  = rrwset_->kvs_[r_idx].off;
      pl->payload = len;

      memcpy(commit_buf_end_ + sizeof(RemoteCommitItem),val,len);
      commit_buf_end_ += (sizeof(RemoteCommitItem) + len);

      // set the lock RPC req fields
      volatile RemoteLockItem *pc = (RemoteLockItem *)lock_buf_end_;
      pc->pid  = rrwset_->kvs_[r_idx].pid;
      pc->off  = rrwset_->kvs_[r_idx].off;
      pc->seq  = m->seq;

      lock_buf_end_ += (sizeof(RemoteLockItem));

      // add the destination server to the broadcast list
      if(server_set_.find(pc->pid) == server_set_.end()) {
        server_set_.insert(pc->pid);
        write_servers_[write_server_num_++] = pc->pid;
      }
      return;
    }

    bool DBFarm::lock_remote(yield_func_t &yield) {

      if(write_items_ > 0) {

#if ATOMIC_LOCK
#if 0
      char *traverse_ptr = lock_buf_ + sizeof(ReqHeader);
      int num_items = write_items_;

      // traverse the lock content
      for(uint i = 0;i < num_items;++i) {

        RemoteLockItem *lh = (RemoteLockItem *)traverse_ptr;
        traverse_ptr += (sizeof(RemoteLockItem));

        // lock the result
        RRWSet::Meta *meta = (RRWSet::Meta *)((char *)base_ptr_ + lh->off);
        volatile uint64_t *lockptr = (volatile uint64_t *)meta;
        assert(lh->off <= (uint64_t)10000000000);

      } // end iteration
#endif


      uint64_t lock_content = ENCODE_LOCK_CONTENT(current_partition, thread_id, cor_id_);
      return rrwset_->lock_remote(lock_content, yield);
#else

        ReqHeader *rh = (ReqHeader *)lock_buf_;
        rh->cor_id = cor_id_;
        rh->num    = write_items_;

        // sending the rpc
        //rpc_handler_->set_msg((char *)lock_buf_);
        //int num_replies = rpc_handler_->send_reqs(RPC_LOCK,lock_buf_end_ - lock_buf_, // id, size
        //                                        reply_buf_,
        //                                      write_servers_,write_server_num_,cor_id_);
        rpc_handler_->prepare_multi_req(reply_buf_,write_server_num_,cor_id_);
        rpc_handler_->broadcast_to(lock_buf_,RPC_LOCK,lock_buf_end_ - lock_buf_,cor_id_,RRpc::REQ,
                                   write_servers_,write_server_num_);

        START(lock);
        worker->indirect_yield(yield);
        END(lock);

#if 1
        LockReplyHeader *replies = (LockReplyHeader *)reply_buf_;
        for(uint i = 0;i < write_server_num_;++i) {
          if(unlikely(replies[i].res == 0)) {
            return false; // lock failed
          }
        }
#endif
#endif // ATMOIC_LOCK

        return true;
      } // end real send case

      return true;  // no remote lock, just pass
    }

    void DBFarm::release_remote() {

      if(write_items_) {
#if ATOMIC_LOCK
        rrwset_->release_remote();
        return;
#endif
        //rpc_handler_->set_msg((char *)lock_buf_);
        //int num_replies = rpc_handler_->send_reqs(RPC_RELEASE,lock_buf_end_ - lock_buf_, // id, size
          //                                        write_servers_,write_server_num_,cor_id_);
        //rpc_handler_->prepare_multi_req(reply_buf_,
        rpc_handler_->broadcast_to(lock_buf_,
                                   RPC_RELEASE,lock_buf_end_ - lock_buf_,
                                   cor_id_,RRpc::REQ,
                                   write_servers_,write_server_num_);

        // re-assign msg buffers
        lock_buf_   = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
        commit_buf_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);

      }
    } // end release all

    void DBFarm::commit_remote() {

      if(write_items_ > 0) {
#if ONE_WRITE == 1
        rrwset_->write_all_back(0,META_LENGTH,commit_buf_);
#else
        // set header fields
        ReqHeader *rh = (ReqHeader *)commit_buf_;
        rh->cor_id = cor_id_;
        rh->num    = write_items_;

        //rpc_handler_->set_msg((char *)commit_buf_);
        //int num_replies = rpc_handler_->send_reqs(RPC_COMMIT,commit_buf_end_ - commit_buf_, // id, size
          //                                        write_servers_,write_server_num_,cor_id_);
        rpc_handler_->broadcast_to(commit_buf_,
                                   RPC_COMMIT,commit_buf_end_ - commit_buf_,cor_id_,RRpc::REQ,
                                   write_servers_,write_server_num_);
#endif  // end RPC write back
        // re-assign msg buffers
        //  worker->indirect_yield(yield);

        lock_buf_   = msg_buf_alloctors[cor_id_].get_req_buf();
        commit_buf_   = msg_buf_alloctors[cor_id_].get_req_buf();
      } // end check whether need remote commits
    }   // end commit all


    // RPC handlers ///////////////////////////////////////////////////////////
    void DBFarm::lock_rpc_handler(int id,int cid, char *msg,void *arg) {

      char *reply_msg = rpc_handler_->get_reply_buf();

      ReqHeader *rh = (ReqHeader *)msg;
      // initilizae with lock success
      ((LockReplyHeader *) reply_msg)->res = 1;

#if 1
      char *traverse_ptr = msg + sizeof(ReqHeader);
      int num_items = rh->num;

      // traverse the lock content
      for(uint i = 0;i < num_items;++i) {

        RemoteLockItem *lh = (RemoteLockItem *)traverse_ptr;
        traverse_ptr += (sizeof(RemoteLockItem));

        // ignore un-related records
        if(lh->pid != current_partition) continue;

        // lock the result
        RRWSet::Meta *meta = (RRWSet::Meta *)((char *)base_ptr_ + lh->off);
        volatile uint64_t *lockptr = (volatile uint64_t *)meta;
#if 1
        if( unlikely( (*lockptr != 0) ||
                      !__sync_bool_compare_and_swap(lockptr,0,
                                                    ENCODE_LOCK_CONTENT(id,thread_id,
                                                                        cid + 1)))) {
          ((LockReplyHeader *) reply_msg)->res = 0;
          break;
        }
#endif
        // further validate the seq
#if 1   // seq check failed
        if( unlikely( meta->seq != lh->seq)) {
          ((LockReplyHeader *) reply_msg)->res = 0;
          break;
        }
#endif
      } // end iteration
#endif
      //rpc_handler_->send_reply(sizeof(LockReplyHeader),id,cid);
      rpc_handler_->send_reply(reply_msg,sizeof(LockReplyHeader),id,cid);
    } // end lock_rpc_handler

    void DBFarm::release_rpc_handler(int id,int cid,char *msg,void *arg) {

      ReqHeader *rh = (ReqHeader *)msg;
      char *traverse_ptr = msg + sizeof(ReqHeader);
      int num_items = rh->num;

      // traverse the lock content
      for(uint i = 0;i < num_items;++i) {

        RemoteLockItem *lh = (RemoteLockItem *)traverse_ptr;
        traverse_ptr += (sizeof(RemoteLockItem));

        // ignore un-related records
        if(lh->pid != current_partition) continue;

        // lock the result
        RRWSet::Meta *meta = (RRWSet::Meta *)((char *)base_ptr_ + lh->off);
        volatile uint64_t *lockptr = (volatile uint64_t *)meta;

        bool s_res = __sync_bool_compare_and_swap(lockptr,
                                                  ENCODE_LOCK_CONTENT(id,thread_id,cid + 1),0);
        //        assert(s_res == false);

      } // end iteration
    }   // release rpc handler

    void DBFarm::commit_rpc_handler(int id,int cid,char *msg,void *arg) {
#if 1
      ReqHeader *rh = (ReqHeader *)msg;
      char *traverse_ptr = msg + sizeof(ReqHeader);
      int num_items = rh->num;

      // traverse the lock content
      for(uint i = 0;i < num_items;++i) {

        RemoteCommitItem *lh = (RemoteCommitItem *)traverse_ptr;
        traverse_ptr += (lh->payload + sizeof(RemoteCommitItem));

        // ignore un-related records
        if(lh->pid != current_partition) continue;

        volatile RRWSet::Meta *m = (RRWSet::Meta *)((char *)base_ptr_ + lh->off);

        /* local writes */
        auto pre_seq = m->seq;
        m->seq = 1;
#if 1
        asm volatile("" ::: "memory");
        memcpy((char *)m + META_LENGTH,(char *)lh + sizeof(RemoteCommitItem),lh->payload);
        asm volatile("" ::: "memory");
#endif
        m->seq = pre_seq + 2;

        /* By the way, release the lock */
        asm volatile("" ::: "memory");
        m->lock = 0;

      } // end iterating commit values
#endif
    } // end commit rpc handler

  }; // namespace db

};   // namespace nocc
