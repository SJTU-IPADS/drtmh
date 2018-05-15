#include "backup_worker.h"
#include "ralloc.h"

#include "ring_msg.h"
#include "ud_msg.h"
#include "util/printer.h"

/* global config constants */
extern size_t nthreads;
extern size_t scale_factor;
extern size_t current_partition;
extern size_t total_partition;

using namespace rdmaio::ringmsg;

namespace nocc {

  extern RdmaCtrl *cm;

  namespace oltp {

    extern View* my_view; // replication setting of the data

    BackupBenchWorker::BackupBenchWorker(unsigned worker_id):
        initilized_(false), worker_id_(worker_id), thread_id_(worker_id + nthreads + 2){

      assert(cm != NULL);
      assert(my_view != NULL);
      cm_ = cm;

      // assert that each backup thread can clean at least one remote thread's log
      assert(worker_id_ < nthreads);
      start_tid_ = worker_id_;
      final_tid_ = nthreads;
      thread_gap_ = backup_nthreads;

      num_nodes_ = cm_->get_num_nodes();

      uint64_t base_offset = DBLogger::get_base_offset();
      base_ptr_ = (char*)cm_->conn_buf_ + base_offset;
      node_area_size_ = THREAD_AREA_SIZE * nthreads;

      feedback_offsets_ = new uint64_t*[num_nodes_];
      head_offsets_ = new uint64_t*[num_nodes_];
      tail_offsets_ = new uint64_t*[num_nodes_];
      for(int n_id = 0; n_id < num_nodes_; n_id++){
        feedback_offsets_[n_id] = new uint64_t[nthreads];
        head_offsets_[n_id] = new uint64_t[nthreads];
        tail_offsets_[n_id] = new uint64_t[nthreads];
        for(int t_id = 0; t_id < nthreads; t_id++){
          feedback_offsets_[n_id][t_id] = base_offset + n_id * node_area_size_ +
                                    t_id * THREAD_AREA_SIZE + current_partition * sizeof(uint64_t);
          head_offsets_[n_id][t_id] = 0;
          tail_offsets_[n_id][t_id] = 0;
        }
      }

    }

    BackupBenchWorker::~BackupBenchWorker(){
      for(int i = 0; i < nthreads; i++){
        delete [] feedback_offsets_[i];
        delete [] head_offsets_[i];
        delete [] tail_offsets_[i];
      }
        delete [] feedback_offsets_;
        delete [] head_offsets_;
        delete [] tail_offsets_;
    }

    void BackupBenchWorker::run() {
      Debugger::debug_fprintf(stdout,"[Backup] worker_id_:%u\n",worker_id_);
      thread_local_init();

      BindToCore(thread_id_);

      //main loop for backup worker
      while(true){
        for(int t_id = start_tid_; t_id < final_tid_; t_id += thread_gap_){

          char *ptr = base_ptr_ + t_id * THREAD_AREA_SIZE + THREAD_META_SIZE;

          for(int n_id = 0; n_id < num_nodes_; n_id++, ptr += node_area_size_){
            // printf("n_id:%d/%d t_id:%d\n", n_id, num_nodes_, t_id);

            assert(tail_offsets_[n_id][t_id] < THREAD_BUF_SIZE);
            volatile char *poll_ptr = (ptr + tail_offsets_[n_id][t_id]);
            uint64_t msg_size = 0;

            char *tailer_ptr = DBLogger::check_log_completion(poll_ptr, &msg_size);

            if(tailer_ptr == NULL){
              continue;
            }

            DBLogger::clean_log((char*)poll_ptr, tailer_ptr);

            // invalid the log, set the log entry to zero
            memset((void*)poll_ptr,0,msg_size + sizeof(uint64_t) + sizeof(uint64_t));

            uint64_t tail_offset = ((tail_offsets_[n_id][t_id] + msg_size
                                + sizeof(uint64_t) + sizeof(uint64_t)) % THREAD_BUF_SIZE);
            tail_offsets_[n_id][t_id] = tail_offset;
            uint64_t head_offset = head_offsets_[n_id][t_id];

            int processed_log_size = (tail_offset >= head_offset)?
                                              (tail_offset - head_offset) :
                                              (THREAD_BUF_SIZE - head_offset + tail_offset);

            assert(processed_log_size >= 0);
            assert(processed_log_size <= THREAD_BUF_SIZE);

            if(unlikely(processed_log_size >= LOG_THRSHOLD)){
              // printf("[back,%d,%d] write -> %lu\n",n_id,t_id, tail_offset);
              Qp* qp = qp_vec_[n_id];
              int flags = IBV_SEND_INLINE;
              if(qp->first_send()){
                flags |= IBV_SEND_SIGNALED;
              }
              if(qp->need_poll()){
                qp->poll_completion();
              }
              qp->rc_post_send(IBV_WR_RDMA_WRITE, (char*)&tail_offset, sizeof(uint64_t),
                                  feedback_offsets_[n_id][t_id] ,flags);


              head_offsets_[n_id][t_id] = tail_offset;
            }
          }
        }
      }
    }

    void BackupBenchWorker::create_qps() {
      // FIXME: current use hard coded dev id and port id
      //int use_port = worker_id_ % 2;
      int use_port = 0;

      int dev_id = cm->get_active_dev(use_port);
      int port_idx = cm->get_active_port(use_port);

      //      fprintf(stdout,"[WORKER %d] start connects QPs, using dev %d, port %d\n",worker_id_
      //,dev_id,port_idx);

      cm->thread_local_init();
      cm->open_device(dev_id);
      cm->register_connect_mr(dev_id); // register memory on the specific device

      // 2 * nthreads for BenchWorker , 1 for BenchListener,
      //TODO: 5 reserved to ensure no conflict with XingDa,
      cm->link_connect_qps(worker_id_ + 2 * nthreads + 1 + 5, dev_id,port_idx, 0,IBV_QPT_RC);
    }


    void BackupBenchWorker::thread_local_init(){
#if SINGLE_MR == 0
      /* create set of qps */
      create_qps();
#endif

      for(int i = 0;i < num_nodes_; i++) {
        Qp *qp = cm_->get_rc_qp(worker_id_ + 2 * nthreads + 1 + 5,i);
        assert(qp != NULL);
        qp_vec_.push_back(qp);
      }

    }

  }

}
