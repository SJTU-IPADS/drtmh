#include "all.h"
#include "bench_micro.h"

#include "app/smallbank/bank_worker.h" // using bank's input generator for test
#include "framework/req_buf_allocator.h"

extern size_t distributed_ratio;

#define NEED_REPLY 0

namespace nocc {

  extern RdmaCtrl *cm;

  namespace oltp {

    extern __thread util::fast_random   *random_generator;
    extern __thread RPCMemAllocator *msg_buf_alloctors;

    extern Breakdown_Timer *send_req_timers;
    extern Breakdown_Timer *compute_timers;

    extern char *rdma_buffer;      // start point of the local RDMA registered buffer
    extern char *free_buffer;      // start point of the local RDMA heap. before are reserved memory
    extern uint64_t r_buffer_size; // total registered buffer size


    namespace micro {

      extern uint64_t working_space;

      txn_result_t MicroWorker::micro_rpc_write_multi(yield_func_t &yield) {

        auto num = distributed_ratio;
        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();
#if NAIVE == 0 // non batch execuation
        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % (working_space - CACHE_LINE_SZ);
          int      pid    = random_generator[cor_id_].next() % num_nodes;

          // align
          //offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          ReadReq *req = (ReadReq *)req_buf;
          req->off = offset;

          rpc_->prepare_multi_req(reply_bufs_[cor_id_],1,cor_id_);
          rpc_->append_req(req_buf,RPC_WRITE,sizeof(ReadReq),cor_id_,RRpc::REQ,pid);
          indirect_yield(yield);
        }
        return txn_result_t(true,1);
#elif NAIVE == 1 // batch execuation

        ReadReqWrapper *req_array = (ReadReqWrapper *)req_buf;
        rpc_->prepare_multi_req(reply_bufs_[cor_id_],num,cor_id_);

        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % (working_space - CACHE_LINE_SZ);
          int      pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          req_array[i].req.off = offset;
          rpc_handler_->append_req((char *)(&(req_array[i])) + sizeof(uint64_t) + sizeof(rpc_header),
                                   RPC_WRITE,sizeof(ReadReq),cor_id_,RRpc::REQ,pid);
        }
        indirect_yield(yield);
        return txn_result_t(true,1);
#elif NAIVE == 2 // add doorbell
        ReadReqWrapper *req_array = (ReadReqWrapper *)req_buf;
        rpc_->prepare_multi_req(reply_bufs_[cor_id_],0,cor_id_);

        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % (working_space - CACHE_LINE_SZ);
          int      pid    = random_generator[cor_id_].next() % num_nodes;

          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          req_array[i].req.off = offset;
          rpc_->append_pending_req((char *)(&(req_array[i])) + sizeof(uint64_t) + sizeof(rpc_header),
                                   RPC_WRITE,sizeof(ReadReq),cor_id_,RRpc::REQ,pid);
        }
        rpc_->flush_pending();
        indirect_yield(yield);
#endif
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rpc_write(yield_func_t &yield) {

        auto size = distributed_ratio;
        assert(size > 0 && size <= MAX_MSG_SIZE);

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();

        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        const  uint64_t write_space = total_free - size;

        static const int num_nodes = cm->get_num_nodes();

        int      pid    = random_generator[cor_id_].next() % num_nodes;
#if 1
        uint64_t offset = random_generator[cor_id_].next() % write_space;
        // align
        offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

        // prepare an RPC header
        ReadReq *req = (ReadReq *)req_buf;
        req->off = offset;
        req->size = size;
#endif
        // send the RPC
        rpc_->prepare_multi_req(reply_bufs_[cor_id_],1,cor_id_);
        rpc_->append_req(req_buf,RPC_WRITE,sizeof(ReadReq) + size,cor_id_,RRpc::REQ,pid);
        indirect_yield(yield);

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rpc_read(yield_func_t &yield) {

        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        auto size = distributed_ratio;
        assert(size > 0 && size <= MAX_MSG_SIZE);
        //char *req_buf = msg_buf_alloctors[cor_id_][0];

        int window_size = 10;
        assert(window_size < 64);
        static __thread char *req_buf = rpc_->get_static_buf(1024);
        char *req_ptr = req_buf;
#if 1
        rpc_->prepare_multi_req(reply_bufs_[cor_id_], window_size,
                                cor_id_);

        for (uint i = 0; i < window_size; ++i) {

          uint64_t offset = random_generator[cor_id_].next() % working_space;
          int pid = random_generator[cor_id_].next() % num_nodes;

          // prepare an RPC header
          //req_array[i].req.off = offset;
          //req_array[i].req.size = size;
          ReadReq *req = (ReadReq *)(req_ptr + rpc_->rpc_padding());
          req_ptr += (sizeof(ReadReq) + rpc_->rpc_padding());

          req->size = size;
          rpc_->append_pending_req((char *)req,
                                   RPC_READ,sizeof(ReadReq),cor_id_,RRpc::REQ,pid);
        }
        rpc_->flush_pending();
#endif
        indirect_yield(yield);
        ntxn_commits_ += window_size - 1;

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rpc_read_multi(yield_func_t &yield) {
        assert(false);
        return txn_result_t(false,1);
      }


      txn_result_t MicroWorker::micro_rpc_stress(yield_func_t &yield) {
        assert(false);
        return txn_result_t(true,0);
      }

      txn_result_t MicroWorker::micro_rpc_scale(yield_func_t &yield) {
        assert(false);
        return txn_result_t(true, 0);
      }

      extern char *test_buf; // used for write
      void MicroWorker::read_rpc_handler(int id,int cid,char *msg,void *arg) {

        char *reply_msg = rpc_->get_reply_buf();
        ReadReq *req = (ReadReq *)msg;

        assert(req->off >= 0 && req->off < r_buffer_size);
        rpc_->send_reply(reply_msg,CACHE_LINE_SZ,id,worker_id_,cid);
      }

      void MicroWorker::various_read_rpc_handler(int id,int cid,char *msg,void *arg) {
        char *reply_msg = rpc_->get_reply_buf();
        ReadReq *req = (ReadReq *)msg;
        rpc_->send_reply(reply_msg,req->size,id,worker_id_,cid);
      }

      void MicroWorker::batch_write_rpc_handler(int id,int cid,char *msg,void *arg) {

        char *reply_msg = rpc_->get_reply_buf();
        char *reply_msg_start = reply_msg;

        ReadReqHeader *reaHeader = (ReadReqHeader *)msg;
        int req_num = reaHeader->num;
        char *traverse_ptr = msg + sizeof(ReadReqHeader);
        assert(req_num == 10);
        for (uint i = 0; i < req_num; i++){
          ReadReq *req = (ReadReq *)traverse_ptr;
          traverse_ptr += (sizeof(ReadReq) + CACHE_LINE_SZ);
          if (req->pid != current_partition)
            continue;
          assert(req->off >= 0 && req->off < r_buffer_size);
#if NAIVE == 4
          //memcpy(test_buf,(char *)req + sizeof(ReadReq),CACHE_LINE_SZ);
#else
          memcpy(test_buf,(char *)req + sizeof(ReadReq),CACHE_LINE_SZ);
#endif
        }
#if NAIVE == 4
#else
        rpc_->send_reply(reply_msg,1,id,worker_id_,cid);
#endif
      }


      void MicroWorker::batch_read_rpc_handler(int id,int cid,char *msg,void *arg) {
        assert(false);
      }

      void MicroWorker::nop_rpc_handler(int id, int cid, char *msg, void *arg) {
        assert(false);
      }

      void MicroWorker::write_rpc_handler(int id,int cid,char *msg, void *arg) {

        char *reply_msg = rpc_->get_reply_buf();
        ReadReq *req = (ReadReq *)msg;

        assert(req->off >= 0 && req->off < r_buffer_size && req->off + req->size < r_buffer_size);

        assert(test_buf != NULL);
        //memcpy(test_buf + req->off, msg + sizeof(ReadReq), req->size);
        rpc_->send_reply(reply_msg,1,id,worker_id_,cid); // a dummy notification
      }

      void MicroWorker::null_rpc_handler(int id, int cid, char *msg, void *arg) {
        // do nothing
        return;
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
