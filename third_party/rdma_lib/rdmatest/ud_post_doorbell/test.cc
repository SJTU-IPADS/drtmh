#include "rdmaio.h"
#include "all.h"

#include "TestConfigXml.h"

#define MAX_SERVERS 64

extern double tput[MAX_SERVERS];
extern TestConfig config;

uint64_t one_second = 0;

int stick_this_thread_to_core(int);

using namespace rdmaio;

void *run_client(void *arg) {

  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */
  // bindToCore(thread_id);
  /*
   * Pinning is useful when running servers on 2 sockets - we want to check
   * if socket 0's cores are getting higher tput than socket 1, which is hard
   * if threads move between cores.
   */
  if(configs.num_threads > 16) {
    printf("main: Client is running on two sockets. Pinning threads"
           " using CPU_SET\n");
    sleep(2);
    stick_this_thread_to_core(thread_id);
  }

  // create qps
  RdmaCtrl *rdma = configs.cm;

  // choose device id
  int use_port = thread_id % 2;

  int dev_id = rdma->get_active_dev(use_port);
  int port_idx = rdma->get_active_port(use_port);

  auto qp_vec = bootstrap_thread_qps(rdma,thread_id,dev_id,port_idx);

  memset((void *) rdma->dgram_buf_, (uint8_t) thread_id + 1, config.user_bufsize_);

  long long rolling_iter = 0;	/* For performance measurement */
  int ud_qp_i = 0;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  int offset = CACHELINE_SIZE;
  while(offset < config.req_length_) {
    offset += CACHELINE_SIZE;
  }
  assert(offset * config.batch_size_ <= config.user_bufsize_);
  RdmaReq reqs[MAX_DOORBELL_SIZE];
  for(int i = 0; i < config.batch_size_; i++){
    reqs[i].flags = 0;
    reqs[i].buf = (uint64_t) (uintptr_t) &rdma->dgram_buf_[offset * i + 8 * 1024 * 1024];
    reqs[i].length = config.req_length_;
  }
  int num_nodes = rdma->get_num_nodes();

  //wait for remote post first recvs
  sleep(5);
  while(1) {
    if(rolling_iter >= M_8) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
        (double) (end.tv_nsec - start.tv_nsec) / 1000000000;

      tput[thread_id] = M_8 / seconds;
      printf("main: Client %d: %.2f Mops\n", thread_id, M_8 / seconds);

      /* Collecting stats at every server can reduce tput by ~ 10% */
      if(thread_id == 0 && rand() % 5 == 0) {
        double total_tput = 0;
        for(int i = 0; i < MAX_SERVERS; i++) {
          total_tput += tput[i];
        }
        printf("---------------main: Total throughput = %.2f\n", total_tput);
      }
      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }
    for(int nid = 0; nid < num_nodes; nid++){
      if(nid == configs.node_id) continue;
      int local_qid = _QP_ENCODE_ID(configs.node_id,ud_qp_i+thread_id*config.ud_per_thread_+UD_ID_BASE);
      for(int i = 0; i < config.batch_size_; i++){
        reqs[i].wr.ud.remote_qid = _QP_ENCODE_ID(nid,thread_id*config.ud_per_thread_+UD_ID_BASE);
      }
      rdma->post_ud_doorbell(local_qid, config.batch_size_, reqs);
      rolling_iter += config.batch_size_;
      // rdma->post_ud(local_qid,reqs);
      // rdma->poll_cq(local_qid);
      // rolling_iter++;
    }
    /* Use a different QP for the next postlist */
    //    qp_i++;
    //if(qp_i == config.rc_per_thread_) {
    //      qp_i = 0;
    //}
  }

  return NULL;
}

void *run_server(void *arg) {

  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */

  // create qps
  RdmaCtrl *rdma = configs.cm;
  // choose device id
  int use_port = thread_id % 2;
  int dev_id = rdma->get_active_dev(use_port);
  int port_idx = rdma->get_active_port(use_port);


  char *registered_buf = (char *)(rdma->dgram_buf_);

  // modify the buffer based on the thread_id and node id
  *((uint64_t *)(registered_buf + sizeof(uint64_t) * thread_id)) = _QP_ENCODE_ID(configs.node_id,
                                                                                 thread_id);

  auto qp_vec = bootstrap_thread_qps(rdma,thread_id,dev_id,port_idx);
  //hard_code
  int recv_qp_id = _QP_ENCODE_ID(configs.node_id, thread_id*config.ud_per_thread_+UD_ID_BASE);
  rdma->init_dgram_recv_qp(recv_qp_id);

  long long rolling_iter = 0; /* For performance measurement */
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);
  fprintf(stdout,"server running\n");
  while(1) {
    //printf("main: server %d: %d\n", thread_id, rdma->conn_buf_[0]);
    if(rolling_iter >= M_2) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) + 
        (double) (end.tv_nsec - start.tv_nsec) / 1000000000;
      printf("main: Client %d: %.2f Mops\n", thread_id, M_2 / seconds);
      rolling_iter = 0;
    
      clock_gettime(CLOCK_REALTIME, &start);
    }
    // qp->poll_recv_cq();
    // rdma->post_ud_recv(qp->qp,
      // (void *) rdma->dgram_buf_, config.userBufSize, rdma->rdma_device_->dgram_buf_mr->lkey);
    // rolling_iter++;
    rolling_iter += rdma->poll_dgram_recv_cqs(recv_qp_id);
  }
}
