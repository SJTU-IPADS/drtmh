#include "tx_config.h"
#include "bench_micro.h"
#include "framework/req_buf_allocator.h"

#include "ralloc.h"

extern size_t current_partition;
extern size_t total_partition;
extern size_t distributed_ratio; // re-use some configure parameters

namespace nocc {

  extern RdmaCtrl *cm;

  namespace oltp {

    extern __thread util::fast_random   *random_generator;
    extern __thread RPCMemAllocator *msg_buf_alloctors;

    extern char *rdma_buffer;      // start point of the local RDMA registered buffer
    extern char *free_buffer;      // start point of the local RDMA heap. before are reserved memory
    extern uint64_t r_buffer_size; // total registered buffer size

    namespace micro {

      extern uint64_t working_space;

      txn_result_t MicroWorker::micro_rdma_atomic(yield_func_t &yield) {

        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        int      pid    = random_generator[cor_id_].next() % num_nodes;
        uint64_t offset = random_generator[cor_id_].next() % (working_space - sizeof(uint64_t));
        // align
        offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

        uint64_t size = sizeof(uint64_t);
        char *local_buf = (char *)Rmalloc(size);
        qps_[pid]->rc_post_fetch_and_add(local_buf,offset,1,IBV_SEND_SIGNALED,cor_id_);
        rdma_sched_->add_pending(cor_id_,qps_[pid]);
        indirect_yield(yield);
        Rfree(local_buf);

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rdma_write(yield_func_t &yield) {

        auto size = distributed_ratio;
        assert(size > 0 && size <= MAX_MSG_SIZE);
#if 1
        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        static __thread char *local_buf = (char *)Rmalloc(4096);

        const int window_size = 64;
        for(uint i = 0;i < window_size;++i) {
          int      pid    = random_generator[cor_id_].next() % num_nodes;
#if READ_RANDOM == 1
          assert(working_space > MAX_MSG_SIZE);
          uint64_t offset = random_generator[cor_id_].next() %
            (working_space - MAX_MSG_SIZE);
          //8 * 1024 * 1024
        // align
        //offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
#else
          uint64_t offset = 4096 * worker_id_;
#endif

#endif
        //char *local_buf = msg_buf_alloctors[cor_id_].get_req_buf();
        //char *local_buf = rdma_buf_ + sizeof(uint64_t) * cor_id_;
          assert(local_buf != NULL);
          auto qp  = qps_[pid];
          int flag = IBV_SEND_SIGNALED;
          if(size <= 64)
            flag |= IBV_SEND_INLINE;
          //if(qp->first_send()) flag |= IBV_SEND_SIGNALED;
          //if(qp->need_poll())  qp->poll_completion();
          qp->rc_post_send(IBV_WR_RDMA_WRITE,local_buf,size,offset,
                           flag,
                           cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
        }
        indirect_yield(yield);
        ntxn_commits_ += (window_size - 1);
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rdma_read(yield_func_t &yield) {
               auto size = distributed_ratio;
        assert(size > 0 && size <= MAX_MSG_SIZE);
#if 1
        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        const int window_size = 10;
        for(uint i = 0;i < window_size;++i) {
      retry:
          int      pid    = random_generator[cor_id_].next() % num_nodes;
          // if(pid == current_partition) goto retry;
#if READ_RANDOM == 1
          uint64_t offset = random_generator[cor_id_].next() %
            //(total_free - MAX_MSG_SIZE);
            working_space;
          // align
          //offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
#else
          uint64_t offset = 4096 * worker_id_;
#endif

#endif
          //char *local_buf = msg_buf_alloctors[cor_id_].get_req_buf();
          //char *local_buf   = msg_buf_alloctors[cor_id_][0];
          //char *local_buf = rdma_buf_ + sizeof(uint64_t) * cor_id_;
          char *local_buf = rdma_buffer + worker_id_ * 4096 + cor_id_ * CACHE_LINE_SZ;
          //char *local_buf = (char *)Rmalloc(size);
          START(post);
          qps_[pid]->rc_post_send(IBV_WR_RDMA_READ,local_buf,size,offset,
                                IBV_SEND_SIGNALED, // flag
                                cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
          END(post);
        }
        indirect_yield(yield);
        ntxn_commits_ += window_size - 1;
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rdma_write_multi(yield_func_t &yield) {

        auto num = distributed_ratio; // number of remote objects to fetch
        assert(num > 0 && num < MAX_REQ_NUM);

        static const uint64_t free_offset = free_buffer - rdma_buffer;
        static const uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        //const uint64_t write_space = total_free - CACHE_LINE_SZ - CACHE_LINE_SZ;
        const uint64_t write_space = working_space - CACHE_LINE_SZ;
        assert(write_space < working_space);

        char *local_buf = (char *)Rmalloc(CACHE_LINE_SZ);
        assert(local_buf != NULL);
#if NAIVE == 0
        for(uint i = 0;i < num;++i) {
          uint64_t offset = random_generator[cor_id_].next() % write_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          //offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          qps_[pid]->rc_post_send(IBV_WR_RDMA_WRITE,local_buf,CACHE_LINE_SZ,offset,
                                  //IBV_SEND_SIGNALED | IBV_SEND_INLINE,
                                  IBV_SEND_SIGNALED,
                                  cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
          indirect_yield(yield);
        }
#elif NAIVE == 1 // + outstanding
        for(uint i = 0;i < num;++i) {
          uint64_t offset = random_generator[cor_id_].next() % write_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          //offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          qps_[pid]->rc_post_send(IBV_WR_RDMA_WRITE,local_buf,CACHE_LINE_SZ,offset,
                                  IBV_SEND_SIGNALED | IBV_SEND_INLINE,
                                  //IBV_SEND_SIGNALED,
                                  cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
        }
        indirect_yield(yield);
#elif NAIVE == 2 // + doorbell
        Qp   *qps[MAX_REQ_NUM];
        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % write_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;

          int flags = 0;
          qps_[pid]->rc_post_pending(IBV_WR_RDMA_WRITE,local_buf,
                                     CACHE_LINE_SZ,
                                     offset,
                                     IBV_SEND_INLINE, // flag, its ok to use zero here. flush pending will add a signal
                                     cor_id_);

          qps[i] = qps_[pid];
        }
        for(uint i = 0;i < num;++i) {
          if(qps[i]->rc_flush_pending())
            rdma_sched_->add_pending(cor_id_,qps[i]);
        }
        indirect_yield(yield);
#elif NAIVE == 3 // + no completion
        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % write_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;

          int flag = IBV_SEND_INLINE;
          auto qp = qps_[pid];
          if(qp->first_send()) flag = IBV_SEND_SIGNALED;
          if(qp->need_poll())  qp->poll_completion();

          qp->rc_post_send(IBV_WR_RDMA_WRITE,local_buf,CACHE_LINE_SZ,
                           offset,flag);
        }
#endif
        Rfree(local_buf);
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rdma_read_multi(yield_func_t &yield) {

        auto num = distributed_ratio; // number of remote objects to fetch
        assert(num > 0 && num <= MAX_REQ_NUM);

        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

#if NAIVE == 0
        // non batch mode
        for (uint i = 0;i < num; ++i) {

          // choose an offset
          // FaSST uses random offset: random offset aligned to 64 byte.
          // So we used the same choice here.
          uint64_t offset = random_generator[cor_id_].next() % working_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
          // post one request
          char *local_buf = (char *)Rmalloc(CACHE_LINE_SZ);
          qps_[pid]->rc_post_send(IBV_WR_RDMA_READ,local_buf,CACHE_LINE_SZ,offset,IBV_SEND_SIGNALED,cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
          indirect_yield(yield); // waiting for completion
          Rfree(local_buf);
        }
#elif NAIVE == 1 // uses batching
        char *bufs[MAX_REQ_NUM];
        // batch mode
        for(uint i = 0;i < num;++i) {
          //char *local_buf = msg_buf_alloctors[cor_id_][i];
          char *local_buf = (char *)Rmalloc(CACHE_LINE_SZ);
          uint64_t offset = random_generator[cor_id_].next() % working_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
          bufs[i] = local_buf;
          qps_[pid]->rc_post_send(IBV_WR_RDMA_READ,local_buf,CACHE_LINE_SZ,offset,IBV_SEND_SIGNALED,cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
        }

        indirect_yield(yield); // waiting for completions
        for(uint i = 0;i < num;++i)
          Rfree(bufs[i]);
        return txn_result_t(true,1);

#elif NAIVE == 2 // add doorbell batching

        char *bufs[MAX_REQ_NUM];
        Qp   *qps[MAX_REQ_NUM];

        for(uint i = 0;i < num;++i) {
          char *local_buf = (char *)Rmalloc(CACHE_LINE_SZ);

          uint64_t offset = random_generator[cor_id_].next() % working_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
          int flags = 0;
          qps_[pid]->rc_post_pending(IBV_WR_RDMA_READ,local_buf,CACHE_LINE_SZ,offset,
                                     0// flag
                                     ,cor_id_);

          qps[i] = qps_[pid];
          bufs[i] = local_buf;
        }
        for(uint i = 0;i < num;++i) {
          if(qps[i]->rc_flush_pending())
            rdma_sched_->add_pending(cor_id_,qps[i]);
        }
        indirect_yield(yield);
        for(uint i = 0;i < num;++i)
          Rfree(bufs[i]);
#endif

        return txn_result_t(true,1);
      }


      txn_result_t MicroWorker::micro_rdma_atomic_multi(yield_func_t &yield) {

        auto num = distributed_ratio; // number of remote objects to fetch
        assert(num > 0 && num < MAX_REQ_NUM);

        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();
        static uint64_t size = sizeof(uint64_t);

#if NAIVE == 0
        // non batch mode
        for (uint i = 0;i < num; ++i) {

          // choose an offset
          // FaSST uses random offset: random offset aligned to 64 byte.
          // So we used the same choice here.
          uint64_t offset = random_generator[cor_id_].next() % working_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
          // post one request
          char *local_buf = (char *)Rmalloc(size);
          qps_[pid]->rc_post_fetch_and_add(local_buf,offset,1,IBV_SEND_SIGNALED,cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
          indirect_yield(yield); // waiting for completion
          Rfree(local_buf);
        }
#elif NAIVE == 1 // uses batching
        char *bufs[MAX_REQ_NUM];
        // batch mode
        for(uint i = 0;i < num;++i) {
          //char *local_buf = msg_buf_alloctors[cor_id_][i];
          char *local_buf = (char *)Rmalloc(size);
          uint64_t offset = random_generator[cor_id_].next() % working_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
          bufs[i] = local_buf;
          qps_[pid]->rc_post_fetch_and_add(local_buf,offset,1,IBV_SEND_SIGNALED,cor_id_);
          rdma_sched_->add_pending(cor_id_,qps_[pid]);
        }

        indirect_yield(yield); // waiting for completions
        for(uint i = 0;i < num;++i)
          Rfree(bufs[i]);

#elif NAIVE == 2 // add doorbell batching

        char *bufs[MAX_REQ_NUM];
        Qp   *qps[MAX_REQ_NUM];

        for(uint i = 0;i < num;++i) {
          char *local_buf = (char *)Rmalloc(size);

          uint64_t offset = random_generator[cor_id_].next() % working_space;
          uint     pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);
          int flags = 0;
          qps_[pid]->rc_post_fetch_and_add(local_buf,offset,1,0,cor_id_);


          qps[i] = qps_[pid];
          bufs[i] = local_buf;
        }
        for(uint i = 0;i < num;++i) {
          if(qps[i]->rc_flush_pending())
            rdma_sched_->add_pending(cor_id_,qps[i]);
        }
        indirect_yield(yield);
        for(uint i = 0;i < num;++i)
          Rfree(bufs[i]);
#endif

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rdma_one_op(yield_func_t &yield) {

        static const int num_nodes = cm->get_num_nodes();
        static uint64_t free_offset = free_buffer - rdma_buffer;
        //static uint64_t total_free  = r_buffer_size - free_offset; // free RDMA space
        static uint64_t total_free = 1024 * 1024 * 8;
        const  int K = 1024;

#if 1   // used too test blueframe problem
        // choose the target
        int target = random_generator[cor_id_].next() % num_nodes;
        //while(this->running) {
        //int target = current_partition;
        Qp *qp = qps_[target];

        // clear pending states
        int send_flag = IBV_SEND_INLINE;
        if(qp->first_send()) send_flag = IBV_SEND_SIGNALED;// | IBV_SEND_INLINE;
        if(qp->need_poll()) { auto ret = qp->poll_completion(); assert(ret == Qp::IO_SUCC); }
        uint64_t offset = sizeof(uint64_t) * worker_id_;
        qp->rc_post_send(IBV_WR_RDMA_WRITE,rdma_buf_ + sizeof(CACHE_LINE_SZ) * cor_id_,
                         sizeof(uint64_t),offset,send_flag);
        //qp->rc_post_send(IBV_WR_RDMA_READ,rdma_buf_,sizeof(uint64_t),offset,send_flag);
        //this->ntxn_commits_ += 1;
        //}
#else   // used to test best RDMA read performance

        char *local_buf = msg_buf_alloctors[cor_id_][0];
        const int window_size = distributed_ratio;
        const uint64_t space  = 8 * 1024 * 1024;

        Qp *qps[64];

        int window_i = 0;
        uint64_t total = 0;
        uint64_t rolling = 0;

        struct timespec start, end;
        clock_gettime(CLOCK_REALTIME, &start);

        while(running) {

          //if(current_partition != 0)continue;
          for(uint i = 0;i < window_size;++i) {

            //int target = 1;
            //int target = i % num_nodes;
          t_retry:
            int target = random_generator[cor_id_].next() % num_nodes;
            if(target == current_partition) goto t_retry;

            Qp *qp = qps_[target];
            //uint64_t remote_offset = 4096 * worker_id_;
            uint64_t remote_offset = random_generator[cor_id_].next() % space;
            qp->rc_post_send(IBV_WR_RDMA_READ,
                             //local_buf + i * sizeof(uint64_t),
                             rdma_buf_ + remote_offset,
                             sizeof(uint64_t),remote_offset, // size, remote address
                             IBV_SEND_SIGNALED);
            qps[i] = qp;
          }
          for(uint i = 0;i < window_size;++i) {
            qps[i]->poll_completion();
          }
          ntxn_commits_ += window_size;
          rolling += window_size;
        }

        return txn_result_t(true,1);
        while(running) {

          if(total >= window_size) {
            qps[window_i]->poll_completion();
            ntxn_commits_ += 1;
          }
        retry:
          START(post);
#if 1
          int target = random_generator[cor_id_].next() % num_nodes;
          if(target == current_partition) goto retry;
#else
          int target = total % total_partition;
#endif
          END(post);

          uint64_t offset = random_generator[cor_id_].next() % space;
          //uint64_t offset = 4096 * worker_id_;

          qps[window_i] = qps_[target];
          qps[window_i]->rc_post_send(IBV_WR_RDMA_READ,
                                      //rdma_buf_ + window_i * sizeof(uint64_t),
                                      rdma_buf_ + offset,
                                      sizeof(uint64_t),offset,
                                      IBV_SEND_SIGNALED);


          total++;
          window_i = (window_i + 1) % window_size;
          rolling += 1;
        }
#endif
        return txn_result_t(true,1);
      }




      txn_result_t MicroWorker::micro_rdma_scale(yield_func_t &yield) {

        int offset = CACHELINE_SIZE;

        if (current_partition != 0) {
          while (1)
            sleep(10);
        }
#if 1
        // random remote id generator
        //uint64_t id;
        //GetAccount(random_generator[cor_id_],&id);
        //int target = AcctToPid(id);
        //srand (time(NULL));
        //int target = rand() % cm->get_num_nodes();
        //                while(1) {
      retry:
        //int target = random_generator[cor_id_].next() % cm->get_num_nodes();
        int target = 1;
        if (target == current_partition)
          goto retry;
#else
        int target = (current_partition + 1) % (cm->get_num_nodes());
#endif

        char *local_buf = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
        Qp *qp = cm_->get_rc_qp(this->worker_id_, target);

        int send_flag = 0;
        if (qp->first_send()) {
          send_flag = IBV_SEND_SIGNALED;
        }
        if (qp->need_poll()) {
          if (qp->poll_completion() != Qp::IO_SUCC) {}
        }

        qp->rc_post_send(IBV_WR_RDMA_WRITE, local_buf, sizeof(uint64_t), offset * cor_id_, send_flag);
        //qp->rc_post_send(IBV_WR_RDMA_READ,local_buf,sizeof(uint64_t),offset * cor_id_,send_flag);
        //this->ntxn_commits_ += 1;
        //}

        return txn_result_t(true, 0);
      }

      txn_result_t MicroWorker::micro_rdma_doorbell_scale(yield_func_t &yield) {
        // char *local_buffer_s = config->buffer + MEM_BUFFER - 1024 * (config->id + 1);
        // char **localBuf = new char*[batchSize];
        // uint64_t *offs  = new uint64_t [batchSize];
        // int *lens = new int[batchSize];

        // for(int i = 0;i < batchSize;++i) {
        //     localBuf[i] = local_buffer_s + i * sizeof(uint64_t);
        //     offs[i] = sizeof(uint64_t) * i + 0;
        //     lens[i] = sizeof(uint64_t);
        // }

        // uint64_t sum = 0;

        // uint64_t range  = MEM_BUFFER / sizeof(uint64_t);

        // uint64_t nb_tx = 0;

        // for(int i = 0;i < batchSize ;++i) {
        //     r->postReq(_QP_ENCODE_ID(1,config->id),localBuf[i],
        //         (fr.next() % range) * sizeof(uint64_t),sizeof(uint64_t),IBV_WR_RDMA_READ,
        //         ((nb_tx & _CASUAL) == 0)?IBV_SEND_SIGNALED:0);
        //     if( (nb_tx & _CASUAL) == 0 && nb_tx > 0)
        //     r->pollCompletion(_QP_ENCODE_ID(1,config->id));
        //     nb_tx ++;
        // }

        return txn_result_t(true, 0);
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
