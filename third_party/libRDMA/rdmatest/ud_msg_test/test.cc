#include "rdmaio.h"
#include "all.h"
#include "ud_msg.h"
#include "ralloc.h"

#include "TestConfigXml.h"

#include <string.h>

#define MAX_SERVERS 64

extern double tput[MAX_SERVERS];
extern TestConfig config;

uint64_t one_second = 0;

int stick_this_thread_to_core(int);

using namespace rdmaio;
using namespace rdmaio::udmsg;

void msg_callback(char *msg,int size) {
  fprintf(stdout,"received %s\n",msg);
  return;
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
  if(configs.num_threads > 16) {
    printf("main: Client is running on two sockets. Pinning threads"
           " using CPU_SET\n");
    sleep(2);
    stick_this_thread_to_core(thread_id);
  }

  // create qps
  RdmaCtrl *cm = configs.cm;

  // choose device id
  int use_port = thread_id % 2;

  int dev_id = cm->get_active_dev(use_port);
  int port_idx = cm->get_active_port(use_port);

  auto qp_vec = bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);

  memset((void *) cm->conn_buf_, (uint8_t) thread_id + 1, config.user_bufsize_);

  long long rolling_iter = 0;	/* For performance measurement */
  int ud_qp_i = 0;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  int offset = CACHELINE_SIZE;
  while(offset < config.req_length_) {
    offset += CACHELINE_SIZE;
  }

  int num_nodes = cm->get_num_nodes();

  //wait for remote post first recvs
  sleep(5);

  RThreadLocalInit();
  UDMsg *msg_handler = new UDMsg(cm,thread_id,16,msg_callback);

  // send one msg
  //char buffer[1024];
  char *buffer = (char *)(Rmalloc(1024));
  sprintf(buffer,"hahahaha\n");
  msg_handler->send_to(1,buffer,strlen(buffer));

  int counter = 0;
  while(1) {
    sprintf(buffer,"hahahaha %d\n",(counter++));
    msg_handler->send_to(1,buffer,strlen(buffer));
    sleep(1);
  }
  return NULL;
}

void *run_server(void *arg) {

  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */

  // create qps
  RdmaCtrl *cm = configs.cm;
  // choose device id
  int use_port = thread_id % 2;
  int dev_id = cm->get_active_dev(use_port);
  int port_idx = cm->get_active_port(use_port);


  char *registered_buf = (char *)(cm->conn_buf_);

  // modify the buffer based on the thread_id and node id
  *((uint64_t *)(registered_buf + sizeof(uint64_t) * thread_id)) = _QP_ENCODE_ID(configs.node_id,
                                                                                 thread_id);

  auto qp_vec = bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);
  //hard_code
  //int recv_qp_id = _QP_ENCODE_ID(configs.node_id, thread_id*config.ud_per_thread_+UD_ID_BASE);
  Qp *qp = cm->get_ud_qp(thread_id);
  //rdma->init_dgram_recv_qp(recv_qp_id);

  long long rolling_iter = 0; /* For performance measurement */
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  RThreadLocalInit();
  UDMsg *msg_handler = new UDMsg(cm,thread_id,16,msg_callback);

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
    //rolling_iter += cm->poll_dgram_recv_cqs(recv_qp_id);
    msg_handler->poll_comps();
  }
}
