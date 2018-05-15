#include "rdmaio.h"
#include "all.h"

#include "TestConfigXml.h"

#define MAX_SERVERS 64

extern double tput[MAX_SERVERS];
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

  // modify the buffer based on the thread_id and node id
  *((uint64_t *)(registered_buf + sizeof(uint64_t) * thread_id)) = _QP_ENCODE_ID(configs.node_id,
                                                                                 thread_id);
  asm volatile("" ::: "memory");
  // then allow connections
  bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);

  // just sleep
  while(1) {
    printf("main: server %d: %d\n", thread_id, cm->conn_buf_[0]);
    sleep(1);
  }
}

void *run_client(void *arg) {

  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */
  // bindToCore(thread_id);
  /*
   * Pinning is useful when running servers on 2 sockets - we want to check
   * if socket 0's cores are getting higher tput than socket 1, which is hard
   * if threads move between cores.
   */
  fprintf(stdout,"check config file at client, at node %d, threads %d\n",configs.id,configs.num_threads);
  if(configs.num_threads > 16) {
    printf("main: Client is running on two sockets. Pinning threads"
           " using CPU_SET\n");
    sleep(2);
    stick_this_thread_to_core(thread_id);
  }

  // create qps
  RdmaCtrl *rdma = configs.cm;
  // choose device id
  int dev_id = 0;
  int port_idx = 1;

  auto qp_vec = bootstrap_thread_qps(rdma,thread_id,dev_id,port_idx);

  memset((void *) rdma->conn_buf_, (uint8_t) thread_id + 1, config.user_bufsize_);

  long long rolling_iter = 0;	/* For performance measurement */
  int qp_i = 0;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  int offset = CACHELINE_SIZE;
  while(offset < config.req_length_) {
    offset += CACHELINE_SIZE;
  }
  fprintf(stdout,"batch %d, bufsize %lu\n",config.batch_size_,config.user_bufsize_);
  assert(offset * config.batch_size_ <= config.user_bufsize_);

  RdmaReq reqs[MAX_DOORBELL_SIZE];

  int num_nodes = rdma->get_num_nodes();

  while(1) {

    char *local_buf = (char *)(rdma->conn_buf_) + 1024 * thread_id;
    *((uint64_t *)local_buf) = 73; // cleaning

    for(int nid = 0; nid < num_nodes; nid++){

      //if(nid == configs.node_id) continue;
      fprintf(stdout,"pinging %d\n", nid);
      //int qid = _QP_ENCODE_ID(nid,qp_i + config.rc_per_thread_ * thread_id + RC_ID_BASE);

      Qp *qp = qp_vec[nid];
      qp->rc_post_send(configs.opcode, local_buf, sizeof(uint64_t),
                       0 + sizeof(uint64_t) * thread_id,IBV_SEND_SIGNALED);
      Qp::IOStatus status = qp->poll_completion(NULL);
      assert(status == Qp::IO_SUCC);
      uint64_t polled_value = *((uint64_t *)local_buf);

      if(nid != configs.node_id) {
        // the server machine will set the memory for the checksum
        assert(_QP_DECODE_INDEX(polled_value) == thread_id);
        assert(_QP_DECODE_MAC(polled_value) == nid);
      } else {
        // my node id will not running server, so it's value is the same as the init value
      }

      fprintf(stdout,"succeed, pinned value %lu @%d\n",polled_value,thread_id);
      rolling_iter++;
    }
    sleep(1);
  }

  return NULL;
}
