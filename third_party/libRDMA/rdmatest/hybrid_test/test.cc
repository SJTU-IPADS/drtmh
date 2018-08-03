#include "rdmaio.h"
#include "all.h"

#include "TestConfigXml.h"

#define MAX_THREADS 64

extern double tput[MAX_THREADS];
extern TestConfig config;

uint64_t one_second = 0;

int stick_this_thread_to_core(int);

using namespace rdmaio;

void *run_server(void *arg) {
  // server thread
  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */

  fprintf(stdout,"run server start...\n");
  // create qps
  RdmaCtrl *cm = configs.cm;
  // choose device id
  int dev_id = 0;
  int port_idx = 1; // seems it shall be greater than 0

  char *registered_buf = (char *)(cm->conn_buf_);

  asm volatile("" ::: "memory");

  
  cm->thread_local_init();
  cm->open_device(dev_id);
  cm->register_connect_mr(dev_id);
  // then allow connections
  // bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);
  bootstrap_rc_qps(cm, thread_id, dev_id, port_idx);

  int offset = CACHELINE_SIZE;
  while(offset < config.req_length_) {
    offset += CACHELINE_SIZE;
  }
  int remote_addr = offset * thread_id;
  // just sleep
  volatile uint64_t* targer_ptr = (volatile uint64_t*)(&cm->conn_buf_[remote_addr]);
  while(1) {
    printf("main: server %d: %x\n", thread_id, *targer_ptr);
    sleep(1);
  }
}

void *run_client(void *arg) {

  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */

  // create qps
  RdmaCtrl *cm = configs.cm;
  // choose device id
  int dev_id = 0;
  int port_idx = 1;

  cm->thread_local_init();
  cm->open_device(dev_id);
  cm->register_connect_mr(dev_id);
  // auto qp_vec = bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);
  auto qp_vec = bootstrap_rc_qps(cm, thread_id, dev_id, port_idx);
  int thread_qp_num = qp_vec[0].size();
  printf("thread_id:%d, thread_qp_num:%d\n", thread_id, thread_qp_num);
  assert(thread_qp_num == config.rc_per_thread_);

  long long rolling_iter = 0;	/* For performance measurement */
  int qp_i = 0;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  int offset = CACHELINE_SIZE;
  while(offset < config.req_length_) {
    offset += CACHELINE_SIZE;
  }
  int remote_addr = offset * thread_id + (configs.node_id + 1) * offset * configs.num_threads;
  // fprintf(stdout,"batch %d, bufsize %lu\n",config.batch_size_,config.user_bufsize_);
  // assert(offset * config.batch_size_ <= config.user_bufsize_);

  // RdmaReq reqs[MAX_DOORBELL_SIZE];
  // for(int i = 0; i < config.batch_size_; i++){
  //   reqs[i].opcode = configs.opcode;
  //   reqs[i].flags = 0;
  //   reqs[i].buf = (uint64_t) (uintptr_t) &rdma->conn_buf_[offset * i];
  //   reqs[i].length = config.req_length_;
  //   reqs[i].wr.rdma.remote_offset = offset * i;
  // }
  int num_nodes = cm->get_num_nodes();
  int iterations_num = config.iters_ * thread_qp_num;
  srand(time(NULL));

  char *local_buf = (char *)(&cm->conn_buf_[thread_id * offset]);
  // *((uint64_t*)local_buf) = thread_id;
  *local_buf = thread_id;
  int target = 0, qp_num = 0;

  while(1) {

    if(rolling_iter >= iterations_num) {

      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
        (double) (end.tv_nsec - start.tv_nsec) / 1000000000;

      tput[thread_id] = iterations_num / seconds;
      printf("main: Client %d: %.2f Mops\n", thread_id, iterations_num / seconds);

      /* Collecting stats at every server can reduce tput by ~ 10% */
      if(thread_id == 0 /*&& rand() % 5 == 0*/) {
        double total_tput = 0;
        for(int i = 0; i < configs.num_threads; i++) {
          total_tput += tput[i];
        }
        printf("---------------main: Total throughput = %.2f, latency = %.6f\n", total_tput, thread_qp_num*configs.num_threads*1000000/total_tput);
      }
      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
      fflush(stdout);
    }

    // target = rand() % num_nodes;
    target = (target + 1) % num_nodes;
    if(target == configs.node_id)
      continue;

    Qp *qp = qp_vec[target][qp_num];
    assert(qp != NULL);

    int send_flag = 0;
    // if(qp->first_send()) {
    //   send_flag = IBV_SEND_SIGNALED;
    // }
    // if(qp->need_poll())
    //   qp->poll_completion();

    send_flag = IBV_SEND_SIGNALED;
    if(!qp->first_send()){
      qp->poll_completion();
    }
    qp->need_poll();
    qp->rc_post_send(IBV_WR_RDMA_WRITE,(char*)local_buf,config.req_length_,remote_addr,send_flag);
    rolling_iter += 1;
    qp_num++;
    if(qp_num == thread_qp_num)
      qp_num = 0;
    // qp->poll_completion();


    // qp_vec[nid]->rc_post_doorbell(reqs,config.batch_size_);
    // rolling_iter += config.batch_size_;
  }

  return NULL;
}
