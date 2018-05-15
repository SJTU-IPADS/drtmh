#include "si_ts_manager.h"
#include "dbsi.h"
#include "ralloc.h"
#include "rdmaio.h"
#include "util/util.h"

#include <pthread.h>

extern size_t current_partition;
extern size_t nthreads;

using namespace std::placeholders;

static ts_manage_func_t moniter;
static ts_manage_func_t poller;

void  *pthread_call_wrapper (void *arg) {
  return moniter(arg);
}

void *pthread_call_wrapper1 (void *arg) {
  return poller(arg);
}

#define unlikely(x) __builtin_expect(!!(x), 0)

using namespace rdmaio;

namespace nocc {

  namespace db {
    __thread char *local_write_buffer = NULL;
#if LARGE_VEC == 1
    __thread uint64_t TSManager::local_timestamp_ = 3;
#endif

    TSManager::TSManager(RdmaCtrl *cm,uint64_t addr,int id,int master_id,int wid)
      : cm_(cm),
        ts_addr_(addr),
        id_(id),
        master_id_(master_id),
        worker_id_(wid),
        fetched_ts_buffer_(NULL),
        total_partition(cm_->get_num_nodes())
    {
#if LARGE_VEC == 0
      local_timestamp_ = 3;
      last_ts_ = local_timestamp_ - 1;
      tv_size_ = this->total_partition * sizeof(uint64_t);
#else
      tv_size_ = this->total_partition * nthreads * sizeof(uint64_t);
#endif
      // Start the monitor
      if(1) {
        poller = std::bind(&TSManager::timestamp_poller,this,_1);
        pthread_t tid;
        pthread_create(&tid,NULL,pthread_call_wrapper1,NULL);
      }
#if 1 // wait for the timestamp to be fetched
      while(fetched_ts_buffer_ == NULL) {
        asm volatile("" ::: "memory");
      }
#endif
    }

    void *TSManager::timestamp_poller(void *) {

      fprintf(stdout,"[TS] Timestamp poller started.\n");
      // Maybe bind?
      RThreadLocalInit();
      assert(total_partition < 64);
      uint64_t *local_buffer = (uint64_t *)Rmalloc(tv_size_);

      uint64_t *fetched_ts = (uint64_t *)(new char[tv_size_]);
      uint64_t *target_ts  = (uint64_t *)(new char[tv_size_]);
      assert(local_buffer != NULL && fetched_ts != NULL && target_ts != NULL);

      // First init the timestamp manager
#if LARGE_VEC == 1
      for(uint i = 0;i < nthreads * total_partition;++i)
        fetched_ts[i] = local_timestamp_ - 1;
#else
      for(uint i = 0;i < total_partition;++i)
        fetched_ts[i] = local_timestamp_ - 1;
#endif

      fetched_ts_buffer_ = (char *)fetched_ts;
      char *temp = (char *)target_ts;

#if 1
      int use_port = 0;
      int dev_id = cm_->get_active_dev(use_port);
      int port_idx = cm_->get_active_port(use_port);

      cm_->thread_local_init();
      cm_->open_device(dev_id);
      cm_->register_connect_mr(dev_id); // register memory on the specific device
      cm_-> // create qp for timestamp fetch
        link_connect_qps(worker_id_, dev_id, port_idx, 0, IBV_QPT_RC);
#endif

      Qp *qp = cm_->get_rc_qp(worker_id_,master_id_);
      while(true) {
        /* keep fetching */
        qp->rc_post_send(IBV_WR_RDMA_READ,(char *)local_buffer,tv_size_,0,IBV_SEND_SIGNALED);
        auto ret = qp->poll_completion();
        if(ret != Qp::IO_SUCC) continue;
        /* ensure an atomic fetch */
        memcpy(temp,local_buffer,tv_size_);
        char *swap = temp;
        temp = fetched_ts_buffer_;
        fetched_ts_buffer_ = swap;
      }
    }

    void TSManager::get_timestamp(char *buffer, int tid) {
      memcpy(buffer,fetched_ts_buffer_,tv_size_);
      return ;
    }

    void TSManager::thread_local_init() {
    }
  };
};
