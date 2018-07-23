#include "all.h"

#include "rpc.h"
#include "ralloc.h"
#include "ring_msg.h"

#include "util/util.h"
#include "util/mapped_log.h"

#include "core/routine.h"

#include <unistd.h>
#include <assert.h>
#include <string.h>

#include <queue>

#include <arpa/inet.h> //used for checksum

#ifdef RPC_TIMEOUT_FLAG
#include<sys/time.h>
#endif

//__thread char temp_msg_buf[MAX_MSG_SIZE];
__thread char *msg_buf = NULL;

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

extern size_t current_partition;

extern size_t nthreads; //For debug!! shall be cleaned

using namespace rdmaio;
using namespace nocc::util;


extern __thread MappedLog local_log;

// some helper functions
static inline uint64_t rad_checksum(void *vdata,size_t length,int rpc_id,int payload);

namespace nocc {

  __thread char        **reply_bufs_ ;
  __thread int          *reply_counts_;
  __thread int          *reply_size_;

#ifdef RPC_TIMEOUT_FLAG
  extern __thread struct  timeval   *routine_timeouts_;
#endif

#ifdef RPC_VERBOSE
  __thread struct rpc_info *rpc_infos_;
#endif

  namespace oltp {

    Rpc::Rpc (MsgHandler *msg,int tid)
      : message_handler_(msg),
        reply_buf_(NULL),
        handled_replies_(0),
        required_replies_(0),
        nrpc_processed_(0),
        nrpc_polled_(0),
        inited_(false),
        thread_id_(tid)
    {
      assert(sizeof(rpc_header::meta) == sizeof(uint32_t));
      clear_reqs();

#ifdef TIMER
      // clear timers
      rpc_timer.report();
      post_timer.report();
      minor_timer.report();
#endif
    }

    void Rpc::thread_local_init(int coroutines) {
#ifdef RPC_TIMEOUT_FLAG
      //routine_timeouts_ = new struct timeval[16];
#endif
#ifdef RPC_VERBOSE
      rpc_infos_ = new rpc_info[16];
#endif

      reply_bufs_       = new char*[1 + coroutines];
      reply_counts_     = new int[1 +   coroutines];
      reply_size_       = new int[1 +   coroutines];

      for(uint i = 0;i < 1 + coroutines;++i){
        reply_counts_[i] = 0;
        reply_bufs_[i]   = NULL;
        reply_size_[i]   = 0;
      }
      registered_rpcs.clear();
    }

    void Rpc::init(int num) {
      //    fprintf(stdout,"thread %d init\n",thread_id_);
      /* alloc a local buffer */
      if(!inited_) {

        for(uint i = 0;i < num;++i) {
          int reply_msg_size = 4000;
          assert(reply_msg_size > 0);
          reply_msg_bufs_[i] = (char *)Rmalloc(reply_msg_size);
          if(reply_msg_bufs_[i] != NULL)
            memset(reply_msg_bufs_[i],0,reply_msg_size);
          else assert(false);
        }
        //      this->current_req_buf_slot_ = 0;
        this->current_reply_buf_slot_ = 0;
        inited_ = true;
      }
    }

    void Rpc::register_callback(rpc_func_t callback, int id) {
      if(registered_rpcs.find(id) != registered_rpcs.end()) {
        //fprintf(stderr,"[WARNING], register RPC id %d duplicated, @%d\n",id,thread_id_);
        return;
      }
      callbacks_[id] = callback;
      registered_rpcs.insert(id);
    }

    void Rpc::clear_reqs() {
    }

    char *Rpc::get_req_buf() {
      assert(false);
      return NULL;
    }

    int Rpc::prepare_multi_req(char *reply_buf,int num_of_replies,int cid) {
      assert(reply_counts_[cid] == 0);
      reply_counts_[cid] = num_of_replies;
      reply_bufs_[cid]   = reply_buf;
#if USE_UD_MSG == 1
      message_handler_->prepare_pending();
#endif
    }

    int Rpc::append_req(char *msg,int rpc_id,int size,int server_id,int server_tid,int cid,int type) {
      //int Rpc::append_req(char *msg,int rpc_id,int size,int server_id,int cid,int type) {

      set_msg(msg);

      // set the RPC header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.type = type;
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

      int total_size = size + sizeof(struct rpc_header);

#if USE_UD_MSG == 0 && USE_TCP_MSG == 0
      *((uint64_t *)msg_buf_) = total_size;
      *((uint64_t *)(msg_buf_ + sizeof(uint64_t) + total_size)) = total_size;

      Qp::IOStatus res = message_handler_->send_to(server_id,(char *)msg_buf_,total_size);
#else
      Qp::IOStatus res = message_handler_->send_to(server_id,server_tid,(char *)(msg_buf_ + sizeof(uint64_t)),total_size);
#endif
      assert(res == Qp::IO_SUCC);

      return 0;
    }

    int Rpc::append_req_ud(char *msg,int rpc_id,int size,int nid,int tid,int cid) {

      set_msg(msg);

      // set the RPC header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.type = REQ;
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

      int total_size = size + sizeof(rpc_header);

      message_handler_->prepare_pending();
      auto res = message_handler_->post_pending(nid,tid,(char *)(msg_buf_ + sizeof(uint64_t)),total_size);
      assert(res == Qp::IO_SUCC);
      return 0;
    }

    int Rpc::append_req_ud(char *msg,int rpc_id,int size,int server_id,int cid) {

      set_msg(msg);

      // set the RPC header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.type = 0;
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

      int total_size = size + sizeof(struct rpc_header);

      message_handler_->prepare_pending();
      Qp::IOStatus res = message_handler_->post_pending(server_id,(char *)(msg_buf_ + sizeof(uint64_t)),total_size);
      message_handler_->flush_pending();
      assert(res == Qp::IO_SUCC);
      return 0;
    }


    int Rpc::_send_reqs(int rpc_id,int size,int *server_lists, int num,int cid,int type) {
      assert(rpc_id <= 32);
#ifdef TIMER
      minor_timer.start();
#endif

#if USE_UD_MSG == 0 && USE_TCP_MSG == 0
      /* align to 8*/
      int total_size = size + sizeof(struct rpc_header);
      total_size = total_size + sizeof(uint64_t) - total_size % sizeof(uint64_t);
#endif
      // set the rpc header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.type = type;
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

#ifdef RPC_CHECKSUM
      header->checksum = rad_checksum((char *)msg_buf_ + sizeof(uint64_t) + sizeof(struct rpc_header),size,
                                      rpc_id,size);
      //header->counter = routine_counters_[cid];
      //      if(thread_id_ != nthreads) fprintf(stdout,"send counter %lu\n",routine_counters_[cid]);
      //routine_counters_[cid] += 1;
#endif

#ifdef TIMER
      minor_timer.end();
#endif

#if USE_UD_MSG == 0  && USE_TCP_MSG == 0 // use RC to post msgs

      *((uint64_t *)msg_buf_) = total_size;
      *((uint64_t *)(msg_buf_ + sizeof(uint64_t) + total_size)) = total_size;

#ifdef TIMER
      post_timer.start();
#endif
      auto ret = message_handler_->broadcast_to(server_lists,num,(char *)msg_buf_, total_size);
      assert(ret == Qp::IO_SUCC);
#else // UD case
      assert(num > 0); // the request server shall be > 0
      auto ret = message_handler_->broadcast_to(server_lists,num,(char *)msg_buf_ + sizeof(uint64_t),
                                                size + sizeof(rpc_header));
      assert(ret == Qp::IO_SUCC);
#endif

#ifdef TIMER
      post_timer.end();
#endif

      return num;
    }

    int Rpc::send_reqs_ud(int rpc_id,int size,int *server_ids,int num,int cid) {
      // set the rpc header
      volatile struct rpc_header *header = (volatile struct rpc_header *)(msg_buf_ + sizeof(uint64_t));
      header->meta.rpc_id  = rpc_id;
      header->meta.payload = size;
      header->meta.cid = cid;

#ifdef RPC_CHECKSUM
      header->checksum = rad_checksum((char *)msg_buf_ + sizeof(uint64_t) + sizeof(struct rpc_header),size,
                                      rpc_id,size);
      //header->counter = routine_counters_[cid];
      //      if(thread_id_ != nthreads) fprintf(stdout,"send counter %lu\n",routine_counters_[cid]);
      //routine_counters_[cid] += 1;
#endif

#ifdef TIMER
      minor_timer.end();
#endif
      auto ret = message_handler_->broadcast_to(server_ids,num,(char *)msg_buf_ + sizeof(uint64_t),
                                                size + sizeof(rpc_header));
      assert(ret == Qp::IO_SUCC);
    }

    int //__attribute__((optimize("O0")))
    Rpc::send_reqs(int rpc_id,int size, char *reply_buf,int *server_ids,int num,
                     int cid,int type) {
      assert(reply_counts_[cid] == 0);
      reply_counts_[cid] = num;
      reply_bufs_[cid]   = reply_buf;

#ifdef RPC_TIMEOUT_FLAG
      // only reply needs to count the start time
      gettimeofday(&(routine_timeouts_[cid]),NULL);
#endif

#ifdef RPC_VERBOSE
      //assert(rpc_id != 0);
      rpc_infos_[cid].rpc_id = rpc_id;
      rpc_infos_[cid].first_node_id = server_ids[0];
#endif

      return _send_reqs(rpc_id,size,server_ids,num,cid,type);
    }

    int Rpc::send_reqs(int rpc_id,int size,int *server_lists,int num,int cid) {

      return _send_reqs(rpc_id,size,server_lists,num,cid);
    }

    bool Rpc::poll_comp_callback(char *msg,int from,int from_t) {

      static int total_servers = message_handler_->get_num_nodes();
      nrpc_polled_ += 1;
      struct rpc_header *header = (struct rpc_header *) msg;
      //      fprintf(stdout,"receive rpc id %d from %d  @%d, type %d\n",header->meta.rpc_id,from,thread_id_,
      //      header->meta.type);

      if(header->meta.type == REQ) {
        // normal rpcs
        try {
#ifdef TIMER
          rpc_timer.start();
#endif
          callbacks_[header->meta.rpc_id](from,header->meta.cid,msg + sizeof(struct rpc_header),
                                          (void *)this);
          nrpc_processed_ += 1;
#ifdef TIMER
          rpc_timer.end();
#endif
        } catch (...) {
          fprintf(stdout,"call rpc failed @thread %d, cid %d, rpc_id %d\n",
                  thread_id_,header->meta.cid,header->meta.rpc_id);
          assert(false);
        }

      } else if (header->meta.type == Y_REQ) {
        // copy the msg
        char *temp = (char *)malloc(header->meta.payload);
        memcpy(temp,msg + sizeof(struct rpc_header),header->meta.payload);
        add_one_shot_routine(from,header->meta.cid,header->meta.rpc_id,temp);

      } else if (header->meta.type == REPLY) {

        // This is a reply
        if(reply_counts_[header->meta.cid] == 0) {
          fprintf(stdout,"tid %d error, cid %d, size %d\n",thread_id_,header->meta.cid,header->meta.payload);
          assert(false);
        }

        processed_buf_ = reply_bufs_[header->meta.cid];
        assert(processed_buf_ != NULL);

        assert(reply_size_[header->meta.cid] + header->meta.payload < MAX_MSG_SIZE);

        memcpy(processed_buf_,msg + sizeof(struct rpc_header),header->meta.payload);
        reply_bufs_[header->meta.cid] += header->meta.payload;
        reply_size_[header->meta.cid] += header->meta.payload;

        reply_counts_[header->meta.cid] -= 1;
        if(reply_counts_[header->meta.cid] == 0){
          reply_bufs_[header->meta.cid] = NULL;
          reply_size_[header->meta.cid] = 0;
          add_to_routine_list(header->meta.cid);
        }
      } else {
        // unknown msg type
        assert(false);
      }
      return true;
    }

    char *Rpc::get_reply_buf() {
      reply_msg_buf_ = reply_msg_bufs_[current_reply_buf_slot_ % MAX_INFLIGHT_REPLY];
      current_reply_buf_slot_ += 1;
      return reply_msg_buf_ + sizeof(uint64_t) + sizeof(struct rpc_header); /* meta data + rpc type */
    }

    void Rpc::send_reply(int size, int server_id,char *buf,int cid) {
      reply_msg_buf_ = buf - sizeof(uint64_t) - sizeof(struct rpc_header);
      return send_reply(size,server_id,cid);
    }

    void Rpc::send_reply(int size,int server_id,int cid) {
      return send_reply(size,server_id,thread_id_,cid);
    }

    void Rpc::send_reply(int size,int server_id,int server_tid,int cid, MsgHandler *handler) {

      struct rpc_header *header = (struct rpc_header *) (reply_msg_buf_ + sizeof(uint64_t));
      header->meta.type = REPLY;
      header->meta.payload = size;
      header->meta.cid = cid;

#ifdef RPC_CHECKSUM
      header->checksum = rad_checksum((char *)reply_msg_buf_ + sizeof(uint64_t) + sizeof(rpc_header),size,
                                      current_partition,size);
#endif

      Qp::IOStatus res = handler->send_to(server_id,server_tid,
                                          (char *)reply_msg_buf_ + sizeof(uint64_t),
                                          size + sizeof(rpc_header));
    }

    void
    Rpc::send_reply(int size,int server_id,int server_tid,int cid) {
#if USE_UD_MSG == 0 && USE_TCP_MSG == 0
      /* again, round up */
      int total_size = size + sizeof(struct rpc_header);
      total_size = total_size + sizeof(uint64_t) -  total_size % sizeof(uint64_t);
      assert(total_size + sizeof(uint64_t) + sizeof(uint64_t) < 4096);
#endif

      struct rpc_header *header = (struct rpc_header *) (reply_msg_buf_ + sizeof(uint64_t));
      header->meta.type = REPLY;
      header->meta.payload = size;
      header->meta.cid = cid;

#ifdef RPC_CHECKSUM
      header->checksum = rad_checksum((char *)reply_msg_buf_ + sizeof(uint64_t) + sizeof(rpc_header),size,
                                      current_partition,size);
#endif

#if USE_UD_MSG == 0 && USE_TCP_MSG == 0// RC msg does need explicit header
      *((uint64_t *)reply_msg_buf_) = total_size;
      *((uint64_t *)(reply_msg_buf_ + sizeof(uint64_t) + total_size)) = total_size;
      assert(total_size < MAX_MSG_SIZE);

      Qp::IOStatus res = message_handler_->send_to(server_id,server_tid,reply_msg_buf_,total_size);
      assert(res == Qp::IO_SUCC);
#else
#if 0
      Qp::IOStatus res = message_handler_->send_to(server_id,server_tid,
                                                   (char *)reply_msg_buf_ + sizeof(uint64_t),
                                                 size + sizeof(rpc_header));
#else // delayed mode. Does not support one-shot request
      Qp::IOStatus res = message_handler_->post_pending(server_id,
                                                        (char *)reply_msg_buf_ + sizeof(uint64_t),
                                                        size + sizeof(rpc_header));
#endif
#endif
    }

    void Rpc::report() {
      fprintf(stdout,"%lu rpc processed\n",nrpc_processed_);
#ifdef TIMER
      fprintf(stdout,"RPC cycles: %f\n",rpc_timer.report());
      fprintf(stdout,"post cycles: %f\n",post_timer.report());
      fprintf(stdout,"minor cycles: %f\n",minor_timer.report());
#endif
      //message_handler_->check();
    }
    /* end namespace rpc */
  };

};


// some helper functions
static inline uint64_t ip_checksum(void* vdata,size_t length) {
  // Cast the data pointer to one that can be indexed.
  char* data = (char*)vdata;

  // Initialise the accumulator.
  uint64_t acc=0xffff;

  // Handle any partial block at the start of the data.
  unsigned int offset=((uintptr_t)data)&3;
  if (offset) {
    size_t count=4-offset;
    if (count>length) count=length;
    uint32_t word=0;
    memcpy(offset+(char*)&word,data,count);
    acc+=ntohl(word);
    data+=count;
    length-=count;
  }

  // Handle any complete 32-bit blocks.
  char* data_end=data+(length&~3);
  while (data!=data_end) {
    uint32_t word;
    memcpy(&word,data,4);
    acc+=ntohl(word);
    data+=4;
  }
  length&=3;

  // Handle any partial block at the end of the data.
  if (length) {
    uint32_t word=0;
    memcpy(&word,data,length);
    acc+=ntohl(word);
  }

  // Handle deferred carries.
  acc=(acc&0xffffffff)+(acc>>32);
  while (acc>>16) {
    acc=(acc&0xffff)+(acc>>16);
  }

  // If the data began at an odd byte address
  // then reverse the byte order to compensate.
  if (offset&1) {
    acc=((acc&0xff00)>>8)|((acc&0x00ff)<<8);
  }

  // Return the checksum in network byte order.
  return htons(~acc);
}


static inline uint64_t rad_checksum(void *vdata,size_t length,int rpc_id,int payload) {
  uint64_t pre = ip_checksum(vdata,length); //pre-compute the checksum of the content
  return pre + rpc_id + payload; // compute the checksum of the header
}
