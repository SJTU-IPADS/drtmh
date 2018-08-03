#include "rdmaio.h"
#include "all.h"

#include "TestConfigXml.h"

using namespace rdmaio;

extern double tput[MAX_SERVERS];
extern TestConfig config;

uint64_t one_second = 0;

void *run_server(void *arg) {
  // server thread
  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id; /* Global ID of this server thread */

  fprintf(stdout,"run server start...\n");
  // create qps
  RdmaCtrl *cm = configs.cm;
  // choose device id
  int dev_id = 0;
  int port_idx = 1; // seems it shall be greater than 0

  char *registered_buf = (char *)(cm->conn_buf_);

  asm volatile("" ::: "memory");

  
  cm->thread_local_init();
  if(config.enable_single_mr_ == 0) {
    cm->open_device(dev_id);
    cm->register_connect_mr(dev_id);
  }
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
    printf("main: server %d: %lx\n", thread_id, *targer_ptr);
    sleep(1);
  }
}

void *run_client(void *arg) {

  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id; /* Global ID of this server thread */
  // bindToCore(thread_id);
  
  /****** create qps ******/
  RdmaCtrl *cm = configs.cm;
  // choose device id
  int dev_id = 0;
  int port_idx = 1;

  cm->thread_local_init();
  cm->open_device(dev_id);
  cm->register_connect_mr(dev_id);

  // auto qp_vec = bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);
  auto qp_vec = bootstrap_rc_qps(cm, thread_id, dev_id, 1);
  printf("thread_id:%d\n", thread_id);

  long long rolling_iter = 0; /* For performance measurement */

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  int num_nodes = cm->get_num_nodes();
  int iterations_num = config.iters_;
  int target = 0;
  uint64_t space = config.user_bufsize_ - sizeof(uint64_t);
  // srand(time(NULL));

  char *local_buf = (char *)(cm->conn_buf_);

  // uint64_t remote_offset[64];
  // for(uint i = 0; i < 64; i++){
  //     remote_offset[i] = 4096 + 4096 * i + 
  //                     configs.node_id * configs.num_threads * sizeof(uint64_t) + 
  //                     thread_id * sizeof(uint64_t);
  // }
  fast_random random_generator(0xdeadbeef);
  while(1) {

    Qp *qps[64];

    if(rolling_iter >= iterations_num) {

      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
        (double) (end.tv_nsec - start.tv_nsec) / 1000000000;

      tput[thread_id] = rolling_iter / seconds;
      printf("main: Client %d: %.2f ops\n", thread_id, rolling_iter / seconds);


      /* Collecting stats at every server can reduce tput by ~ 10% */
      if(thread_id == 0 /*&& rand() % 5 == 0*/) {
        double total_tput = 0;
        for(int i = 0; i < configs.num_threads; i++) {
          total_tput += tput[i];
        }
        printf("%.0f----\n", total_tput);
      }
      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
      fflush(stdout);
    }

    for(uint i = 0;i < config.batch_size_;++i) {
      int target = random_generator.next() % num_nodes;
      Qp *qp = qp_vec[target];
      uint64_t remote_offset = random_generator.next() % space;
      qp->rc_post_send(IBV_WR_RDMA_READ,local_buf + i * sizeof(uint64_t),sizeof(uint64_t),remote_offset,
                       IBV_SEND_SIGNALED);
      qps[i] = qp;
    }
    for(uint i = 0;i < config.batch_size_;++i) {
      qps[i]->poll_completion();
    }
    rolling_iter += config.batch_size_;
  }

  return NULL;
}
