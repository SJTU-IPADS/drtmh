#include "memstore/memstore.h"

#include "core/routine.h"

#include "framework/bench_worker.hpp"
#include "framework/req_buf_allocator.h"

#include "ralloc.h" // for Rmalloc

#include "util/mapped_log.h" // for logging
#include "db_logger.h"       // for some log utils

#include "remote_set.h"

#include <sstream>

#define MAXSIZE 16

#define unlikely(x) __builtin_expect(!!(x), 0)

#define RAD_LOG 0
extern __thread MappedLog local_log;

extern size_t current_partition;

using namespace nocc::util;

namespace nocc {

  extern __thread oltp::BenchWorker* worker;
  extern __thread oltp::RPCMemAllocator *msg_buf_alloctors;

  extern __thread coroutine_func_t *routines_;

  extern RdmaCtrl *cm;

  namespace oltp {
    extern int rep_factor;
    extern View* my_view; // replication setting of the data
  }

  namespace db {

    RemoteSet::RemoteSet(RRpc *rpc,int cid,int tid)
      : elems_(0),
        reply_buf_size_(0),
        rpc_(rpc),
        meta_size_(0), cor_id_(cid),tid_(tid),
        count_(0),
        need_validate_(false)
    {
      max_length_ = MAXSIZE;
      kvs_   = new RemoteSetItem[max_length_];
      for(uint i = 0;i < max_length_;++i) {
        kvs_[i].pid = -1;
        kvs_[i].tableid = -1;
        kvs_[i].val = NULL;
      }
      reply_buf_       = (char *)malloc(MAX_MSG_SIZE);
      reply_buf_end_   = reply_buf_;

      reply_buf1_      = (char *)malloc(MAX_MSG_SIZE);

      write_server_num_ = 0;
      read_server_num_  = 0;
      server_set_.clear();

      request_buf_ = NULL;
      lock_request_buf_ = NULL;
      write_back_request_buf_ = NULL;

      // init local buffers
      read_server_num_ = 1; // dummy val to let clear_for_reads init, it will be reset to 0 after clear_for_reads

      request_buf_ = rpc_->get_static_buf(MAX_MSG_SIZE);
      lock_request_buf_ = rpc_->get_static_buf(MAX_MSG_SIZE);
      write_back_request_buf_ = rpc_->get_static_buf(MAX_MSG_SIZE);
      log_buf_ = rpc_->get_static_buf(MAX_MSG_SIZE);

      update_write_buf();
      // clear constants
      clear();

      INIT_LAT_VARS(lock);
    }

    int  RemoteSet::do_reads(int tx_id) {

      if(unlikely(elems_ >= std::numeric_limits<uint16_t>::max())) {
        fprintf(stdout,"overflow of items %d\n",elems_);
        sleep(1);
        exit(-1);
      }
      assert(read_server_num_ > 0);
      ((RequestHeader *)request_buf_)->cor_id = cor_id_;
      ((RequestHeader *)request_buf_)->num = elems_;

      read_items_ = elems_;

      //auto ret =  rpc_handler_->send_reqs(RPC_READ,request_buf_end_ - request_buf_,reply_buf_,
      //read_servers_,read_server_num_,cor_id_);

      rpc_->prepare_multi_req(reply_buf_,read_server_num_,cor_id_);
      rpc_->broadcast_to(request_buf_,RPC_READ,
                         request_buf_end_ - request_buf_, // size
                         cor_id_,RRpc::REQ,
                         read_servers_,read_server_num_); // server list
      // prepare another buffer, delayed to validate phase
      //clear_for_reads();
      return read_server_num_;
    }

    inline bool RemoteSet::_check_res(int num_replies) {

      char *ptr = reply_buf1_;

      for(uint i = 0;i < num_replies;++i) {
        if( ((ReplyHeader *)ptr)->num_items_ == 0) {
          return false;
        }
        max_time_ = MAX(max_time_,((ReplyHeader *)ptr)->payload_);
        ptr += sizeof(ReplyHeader);
      }
      return true;
    }

    bool RemoteSet::lock_remote(yield_func_t &yield) {

      if(write_items_ > 0) {
        RequestHeader *reqh = (RequestHeader *)lock_request_buf_;
        reqh->num = write_items_;
        reqh->cor_id  = cor_id_;

        rpc_->prepare_multi_req(reply_buf1_,read_server_num_,cor_id_);
        rpc_->broadcast_to(lock_request_buf_,RPC_LOCK,
                           lock_request_buf_end_ - lock_request_buf_, // size
                           cor_id_,RRpc::REQ,
                           write_servers_,write_server_num_); // server list

        //rpc_handler_->send_reqs(RPC_LOCK,
        //lock_request_buf_end_ - lock_request_buf_, reply_buf1_,
        //                      write_servers_,write_server_num_,cor_id_);
        int num_replies = write_server_num_;
        START(lock);
        worker->indirect_yield(yield);
        END(lock);
        return _check_res(num_replies);
      }

      return true;
    }

    bool RemoteSet::validate_remote(yield_func_t &yield) {

      bool ret = true;
      if(need_validate_ && read_server_num_ > 0 && read_items_ > 0) {

        //assert(request_buf_end_ - request_buf_ < 1024);
        //int num_replies = rpc_handler_->send_reqs(RPC_VALIDATE,request_buf_end_ - request_buf_,reply_buf1_,
        //read_servers_,read_server_num_,cor_id_);
        rpc_->prepare_multi_req(reply_buf1_,read_server_num_,cor_id_);
        rpc_->broadcast_to(request_buf_,RPC_VALIDATE,
                           request_buf_end_ - request_buf_, // size
                           cor_id_,RRpc::REQ,
                           read_servers_,read_server_num_);
        int num_replies = read_server_num_;

        worker->indirect_yield(yield);
        assert(worker->cor_id_ == cor_id_);
        ret = _check_res(num_replies);
        need_validate_ = false;
      }

      return ret;
    }

    void RemoteSet::release_remote(yield_func_t &yield) {

      if(write_items_ > 0) {
        RequestHeader *reqh = (RequestHeader *)lock_request_buf_;
        // set header
        reqh->cor_id = cor_id_;
        reqh->num = write_items_;

        rpc_->prepare_multi_req(reply_buf_,write_server_num_,cor_id_);
        rpc_->broadcast_to(lock_request_buf_,RPC_RELEASE,
                           lock_request_buf_end_ - lock_request_buf_, // size
                           cor_id_,RRpc::REQ,
                           write_servers_,write_server_num_);

        //        rpc_handler_->set_msg((char *)lock_request_buf_);
        //rpc_handler_->send_reqs(RPC_RELEASE, lock_request_buf_end_ - lock_request_buf_,reply_buf_,
        //write_servers_,write_server_num_,cor_id_);
        worker->indirect_yield(yield);
        this->update_write_buf();
      }
    }

    void RemoteSet::log_remote(yield_func_t &yield) {

      if(write_items_ > 0 && rep_factor > 0) {
        // set up remote numbers
        int *log_macs = new int[rep_factor * write_server_num_];
        int  num_logs = 0;
#if EM_FASST
        num_logs = my_view->get_backup(current_partition,log_macs);
#else
#endif
        // re-use log request buf
        volatile RequestHeader *reqh = (volatile RequestHeader *)write_back_request_buf_;
        reqh->padding = max_time_; // max time is the desired sequence
        reqh->cor_id  = cor_id_;
        reqh->num = write_items_;

        rpc_->prepare_multi_req(reply_buf_,num_logs,cor_id_);
        rpc_->broadcast_to(write_back_request_buf_,RPC_LOGGING,
                           write_back_request_buf_end_ - write_back_request_buf_,
                           cor_id_,RRpc::REQ,
                           log_macs,num_logs);
        worker->indirect_yield(yield);
        delete log_macs;
      }
    }

    void
    RemoteSet::commit_remote(yield_func_t &yield) {

      if(write_items_ > 0) {

        volatile RequestHeader *reqh = (volatile RequestHeader *)write_back_request_buf_;

        reqh->padding = max_time_; // max time is the desired sequence
        reqh->cor_id  = cor_id_;
        reqh->num = write_items_;

        assert(reqh->num > 0);

        rpc_->prepare_multi_req(reply_buf_,write_server_num_,cor_id_);
        rpc_->broadcast_to(write_back_request_buf_,RPC_COMMIT,
                           write_back_request_buf_end_ - write_back_request_buf_,
                           cor_id_,RRpc::REQ,
                           write_servers_,write_server_num_);

        //rpc_handler_->set_msg((char *)write_back_request_buf_);
        //rpc_handler_->send_reqs(RPC_COMMIT,write_back_request_buf_end_ - write_back_request_buf_,
        //          reply_buf_, write_servers_,write_server_num_,cor_id_);

        this->update_write_buf();
        worker->indirect_yield(yield);
      }
    }

    void
    RemoteSet::commit_remote_naive(yield_func_t &yield) {

      if(write_items_ > 0) {

        volatile RequestHeader *reqh = (volatile RequestHeader *)write_back_request_buf_;

        reqh->padding = max_time_; // max time is the desired sequence
        reqh->cor_id  = cor_id_;
        reqh->num = write_items_;

        assert(reqh->num > 0);

        rpc_->prepare_multi_req(reply_buf_,write_server_num_,cor_id_);
        rpc_->broadcast_to(write_back_request_buf_,RPC_COMMIT,
                           write_back_request_buf_end_ - write_back_request_buf_,
                           cor_id_,RRpc::REQ,
                           write_servers_,write_server_num_);

        //rpc_handler_->set_msg((char *)write_back_request_buf_);
        //rpc_handler_->send_reqs(RPC_COMMIT,write_back_request_buf_end_ - write_back_request_buf_,
        //          reply_buf_, write_servers_,write_server_num_,cor_id_);

        this->update_write_buf();
        worker->indirect_yield(yield);
      } else {

      }
    }

    bool RemoteSet::get_results_readlock(int num_replies) {

      bool ret  = true;
      char *ptr = reply_buf_;
      RemoteSetRequestItem *requests = (RemoteSetRequestItem *)(request_buf_ + sizeof(RequestHeader) + meta_size_);

      //RemoteLockItem *p = (RemoteLockItem *)lock_request_buf_end_;

      for(uint i = 0;i < num_replies;++i) {

        ReplyHeader *r_header = (ReplyHeader *)ptr;
        int num_entries = r_header->num_items_;
        //        fprintf(stdout,"get num entries %d\n", num_entries);
        assert(num_entries > 0);
        ptr += sizeof(ReplyHeader);

        for(uint j = 0;j < num_entries;++j) {
          RemoteSetReplyItem *pr = (RemoteSetReplyItem *)ptr;
          if(unlikely(pr->seq == 0)) {
            ret = false;

#if 0
            RemoteLockItem *p = (RemoteLockItem *)lock_request_buf_end_;
            p->pid = kvs_[pr->idx].pid;
            p->node = pr->node;
            lock_request_buf_end_ += sizeof(RemoteLockItem);

            write_items_ += 1;
#endif
          }

          kvs_[pr->idx].val = ptr + sizeof(RemoteSetReplyItem);
          kvs_[pr->idx].seq = pr->seq;
          assert(pr->seq != 1);
#if 1
          kvs_[pr->idx].node = pr->node;
#endif
          requests[pr->idx].node = pr->node;
          requests[pr->idx].seq  = pr->seq;
          ptr += (sizeof(RemoteSetReplyItem) + pr->payload);
        }
      }
      reply_buf_size_ = (ptr - reply_buf_);
      // clear the server set for further writes
      server_set_.clear();
      return ret;
    }

    bool RemoteSet::get_result_imm(int idx,char **ptr,int size) {

      // ReplyItem records the sequence and the MemNode
      ReplyItem *header = (ReplyItem *)reply_buf_end_;

      // parse the Memnode
      kvs_[idx].node = header->node;
      kvs_[idx].seq  = header->seq;

      *ptr = reply_buf_end_ + sizeof(ReplyItem);
      reply_buf_end_ += (sizeof(ReplyItem) + size);

      return true;
    }

    bool RemoteSet::get_result_imm_batch(int start_idx,RemoteReqObj *reqs,int num) {
      // FIXME: actually I assume the start_idx == 0,
      // since otherwise the reply buf may be hard to use

      char *traverse_ptr = reply_buf_;

      for(uint i = 0;i < num;++i) {
        ReplyItem *header = (ReplyItem *)traverse_ptr;
        // parse the Memnode
        auto idx = header->idx;

        kvs_[idx].node = header->node;
        kvs_[idx].seq  = header->seq;

        // parse the value
        kvs_[idx].val  = traverse_ptr + sizeof(ReplyItem);

        // update the traverse ptr
        traverse_ptr += (sizeof(ReplyItem) + reqs[i].size);

      } // end iterating the res
      return true;
    }

    bool RemoteSet::get_results(int num_replies) {

      // Got replies
      // Traverse the reply buffer
      char *ptr = reply_buf_;
      RemoteSetRequestItem *requests = (RemoteSetRequestItem *)(request_buf_ +
                                                                sizeof(RequestHeader) + meta_size_);
      int c(0);
      for(uint i = 0;i < num_replies;++i) {
        ReplyHeader *r_header = (ReplyHeader *)ptr;
        int num_entries = r_header->num_items_;
        ptr += sizeof(ReplyHeader);
        for(uint j = 0;j < num_entries;++j) {

          RemoteSetReplyItem *pr = (RemoteSetReplyItem *)ptr;
          kvs_[pr->idx].val = (ptr + sizeof(RemoteSetReplyItem));
          kvs_[pr->idx].seq = pr->seq;
#if 0
          kvs_[pr->idx].node = pr->node;
#endif
          requests[pr->idx].node = pr->node;
          requests[pr->idx].seq  = pr->seq;
          ptr += (sizeof(RemoteSetReplyItem) + pr->payload);

          c += 1;
        }
      }
      reply_buf_size_ = (ptr - reply_buf_);
      // clear the server set for further writes
#if 0
      elems_ = 0;
      read_server_num_ = 0;
      server_set_.clear();
      request_buf_end_ = request_buf_ + sizeof(RequestHeader);
#endif
      return true;
    }

    void RemoteSet::do_reads(yield_func_t &yield) {
      assert(false);// this call shall never be used
      int num_replies = do_reads();
      worker->indirect_yield(yield);
      get_results(num_replies);
    }

    void
    RemoteSet::promote_to_write(int id, char *val, int len) {

      write_items_ += 1;
      read_items_  -= 1;
      assert(read_items_ >= 0);
#if 1
      volatile RemoteLockItem *p = ( volatile RemoteLockItem *)lock_request_buf_end_;
      p->pid  = kvs_[id].pid;
      p->node = kvs_[id].node;
      p->seq  = kvs_[id].seq;
      p->tableid = kvs_[id].tableid;
      lock_request_buf_end_ += sizeof(RemoteLockItem);
#endif

#if 1
      // prepare payload
      volatile RemoteWriteItem *p1 = (volatile RemoteWriteItem *)write_back_request_buf_end_;
      p1->payload = len;
      //p1->node    = kvs_[id].node;
      p1->key     = kvs_[id].key;
      p1->pid     = kvs_[id].pid;
      p1->tableid = kvs_[id].tableid;

      if(len != 0) {
        memcpy((char *)p1 + sizeof(RemoteWriteItem), val, len);
      }
      write_back_request_buf_end_ += (sizeof(RemoteWriteItem) + len);
#endif
      if(write_server_set_.find(kvs_[id].pid) == write_server_set_.end()) {
        write_server_set_.insert(kvs_[id].pid);
        write_servers_[write_server_num_++] = kvs_[id].pid;
      }
    }

    void RemoteSet::write(int8_t tableid,uint64_t key,char *val,int len) {
      assert(false);
#if 0
      for(uint i = 0;i < elems_;++i) {
        if(kvs_[i].tableid == tableid && kvs_[i].key == key) {
          write_items_ += 1;
          RemoteLockItem *p = (RemoteLockItem *)lock_request_buf_end_;
          p->pid = kvs_[i].pid;
          p->node = kvs_[i].node;
          p->seq  = kvs_[i].seq;
          lock_request_buf_end_ += sizeof(RemoteLockItem);

          /* prepare payload */
          RemoteSetReplyItem *p1 = (RemoteSetReplyItem *)write_back_request_buf_end_;
          p1->payload = len;
          p1->node    = kvs_[i].node;
          p1->pid     = kvs_[i].pid;
          memcpy((char *)p1 + sizeof(RemoteSetReplyItem), val, len);
          write_back_request_buf_end_ += (sizeof(RemoteSetReplyItem) + len);
          return ;
        }
      }
#endif
    }

    void RemoteSet::reset() {
      elems_ = 0;
      need_validate_ = false;
    }


    void RemoteSet::update_read_buf() {
      assert(false);
      request_buf_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
      request_buf_end_ = request_buf_ + sizeof(RequestHeader);
    }

    void RemoteSet::clear_for_reads() {
      assert(false);
      if(read_server_num_ > 0 || write_server_num_ > 0) {
        update_read_buf();
        read_items_ = 0;
      }
    }

    void RemoteSet::update_write_buf() {
      // allocate buffers
      //lock_request_buf_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
      lock_request_buf_end_   = lock_request_buf_ + sizeof(RequestHeader);

      //write_back_request_buf_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
      write_back_request_buf_end_ = write_back_request_buf_ + sizeof(RequestHeader);
    }

    void RemoteSet::clear(int meta_len) {

      elems_ = 0; // clear remote rw-set item
      read_items_ = 0; // clear remote read items
      read_server_num_ = 0; // clear remote read servers
      write_items_ = 0;     // clear remote write items
      write_server_num_ = 0;

      // meta ptrs
      meta_size_ = meta_len;
      request_buf_end_ = ((char *)(request_buf_) + sizeof(RequestHeader) + meta_size_);
      reply_buf_end_ = reply_buf_;

      need_validate_ = false; // reset validation status

      server_set_.clear();
      write_server_set_.clear();
    }

    void RemoteSet::set_lockpayload(uint64_t payload)  {
      assert(false);
      //((struct RequestHeader *)write_back_request_buf_)->padding = payload;
    }

    int RemoteSet::add_range(int pid, int8_t tableid, uint64_t *min, uint64_t *max, int klen,int num) {
      assert(num > 1);
      int cur = elems_;
      elems_ += num;

      /* add one to request buf */
      ((struct RequestHeader *)request_buf_)->num += num;
      RemoteSetRequestItem *p = (RemoteSetRequestItem *) request_buf_end_;

      for(uint i = 0;i < elems_;++i) {
        kvs_[i].seq = 0;

        RemoteSetRequestItem *p = (RemoteSetRequestItem *) request_buf_end_;
        memset(p,0,sizeof(RemoteSetRequestItem));

        p->pid = pid;
        p->tableid = tableid;
        p->type = REQ_RANGE_SEARCH;
        p->idx  = cur + i;

        request_buf_end_ += sizeof(RemoteSetRequestItem);
      }
#if LONG_KEY == 1
      memcpy(p[0].key.long_key,(char *)min,klen);
      memcpy(p[1].key.long_key,(char *)max,klen);
#endif

      if(server_set_.find(pid) == server_set_.end()) {
        server_set_.insert(pid);
        read_servers_[read_server_num_++] = pid;
      }

    }

    int RemoteSet::add_imm(REQ_TYPE type,int pid,int8_t tableid,uint64_t key) {
      // record locally
      assert(elems_ + 1 <= max_length_);
      int cur = elems_++;

      kvs_[cur].pid = pid;
      kvs_[cur].tableid = tableid;
      kvs_[cur].key     = key;
      kvs_[cur].val     = NULL;

      // prepare the RPC request
      RequestItemWrapper *req_array = (RequestItemWrapper *)((char *)request_buf_);

      req_array[cur].req.tableid = tableid;
      req_array[cur].req.key     = key;

      // send the req
      rpc_->prepare_multi_req(reply_buf_end_,1,cor_id_);
      rpc_->append_req((char *)(&(req_array[cur])) + sizeof(RRpc::rrpc_header),
                       RPC_READ,sizeof(RequestItem),cor_id_,RRpc::REQ,pid);

      return cur;
    }

    int RemoteSet::add_batch_imm(REQ_TYPE type,RemoteReqObj *reqs,int num) {

      // request buffers
      RequestItemWrapper *req_array = (RequestItemWrapper *)((char *)request_buf_);

      auto ret = elems_; // start remote-set idx of this batched reqs

      rpc_->prepare_multi_req(reply_buf_end_,num,cor_id_);

      for(uint i = 0;i < num;++i) {

        auto pid = reqs[i].pid;
        auto key = reqs[i].key;
        auto tableid = reqs[i].tableid;

        int cur = elems_++;
        kvs_[cur].pid = pid;
        kvs_[cur].tableid = tableid;
        kvs_[cur].key     = key;
        kvs_[cur].val     = NULL;

        req_array[cur].req.tableid = tableid;
        req_array[cur].req.key     = key;
        req_array[cur].req.idx     = cur;

        //rpc_handler_->append_req((char *)(&(req_array[cur])) + sizeof(rrpc_header)
        //                       ,RPC_READ,sizeof(RequestItem),cor_id_,RRpc::REQ,pid,tid_);
        reply_buf_end_ += (sizeof(ReplyItem) + reqs[i].size);

      }    // end iterating batch read reqs
      // TODO!! UD needs an end msg, if possible
      return ret;
    }

    int  RemoteSet::add(REQ_TYPE type,int pid,int8_t tableid,uint64_t *key,int klen) {
      assert(elems_ + 1 <= max_length_);
      assert(type !=  REQ_READ_LOCK);
      assert(pid != current_partition);

      int cur = elems_;
      elems_ ++;
      kvs_[cur].pid = pid;
      kvs_[cur].tableid = tableid;
      kvs_[cur].key = (uint64_t )key;
      kvs_[cur].val = NULL;
      kvs_[cur].seq = 0;

      /* add one to request buf */
      //((struct RequestHeader *)request_buf_)->num += 1;
      RemoteSetRequestItem *p = (RemoteSetRequestItem *) request_buf_end_;
      request_buf_end_ += sizeof(RemoteSetRequestItem);
      memset(p,0,sizeof(RemoteSetRequestItem));
      p->pid = pid;
      p->tableid = tableid;
      assert(klen <= 40);
#if LONG_KEY == 1
      memcpy((p->key.long_key),(char *)key,klen);
#endif
      p->idx  = cur;
      p->type = type;

      if(server_set_.find(pid) == server_set_.end()) {
        server_set_.insert(pid);
        read_servers_[read_server_num_++] = pid;
      }
      return cur;
    }

    int
    RemoteSet::add(REQ_TYPE type,int pid,int8_t tableid,uint64_t key) {

      assert(elems_ + 1 <= max_length_);
      int cur = elems_;
      elems_++;
      kvs_[cur].pid = pid;
      kvs_[cur].tableid = tableid;
      kvs_[cur].key     = key;
      kvs_[cur].val     = NULL;

      /* add one to request buf */
      //((struct RequestHeader *)request_buf_)->num += 1;
      RemoteSetRequestItem *p = (RemoteSetRequestItem *) request_buf_end_;
      request_buf_end_ += sizeof(RemoteSetRequestItem);

      p->pid = pid;
      p->tableid = tableid;
#if LONG_KEY == 1
      p->key.short_key  = key;
#else
      p->key = key;
#endif
      p->idx  = cur;
      p->type = type;

      if(server_set_.find(pid) == server_set_.end()) {
        server_set_.insert(pid);
        read_servers_[read_server_num_++] = pid;
      }
      return cur;
    }

    int RemoteSet::add_update_request(int pid, int8_t tableid, uint64_t key) {
      return add(REQ_READ_LOCK,pid,tableid,key);
    }

    int RemoteSet::add_read_request(int pid, int8_t tableid, uint64_t key) {
      //read_items_ += 1;
      return add(REQ_READ,pid,tableid,key);
    }

    /* helper functions */
    void RemoteSet::print_write_server_list() {
      std::stringstream ss;
      for(uint i = 0; i < write_server_num_;++i) {
        ss << write_servers_[i] << " ";
      }
      fprintf(stdout,"write servers %s @%d\n", ss.str().c_str(),cor_id_);
    }

  };

  int LogHelper::mac_num = 0;
  int LogHelper::threads = 0;
  int LogHelper::coroutine_num = 0;

  int LogHelper::mac_log_area = 0;
  int LogHelper::thread_log_area = 0;

  int LogHelper::max_log_size = 0;

  char *LogHelper::log_base_ptr = NULL;

  void RemoteSet::log_rpc_handler(int id,int cid,char *msg,void *arg) {

    char *thread_ptr = LogHelper::get_thread_ptr(tid_);
    uint64_t size = (uint64_t)arg;
    assert(size < MAX_MSG_SIZE);

    char *log_area = thread_ptr + LogHelper::get_off(id,cid - 1);

    //assert(log_area - (char *)((char *)(cm->conn_buf_) + HUGE_PAGE_SZ) < LogHelper::get_log_size());
    //assert(thread_ptr != NULL);

    memcpy(log_area,msg,size);
    char* reply_msg = rpc_->get_reply_buf();
    rpc_->send_reply(reply_msg,0,id,cid); // a dummy reply
  }

};
