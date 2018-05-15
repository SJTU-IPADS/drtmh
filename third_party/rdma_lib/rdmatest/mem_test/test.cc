// mr tests will only tests memory registriation performance

#include "ralloc.h"
#include "rdmaio.h"

#include "all.h"

#include "TestConfigXml.h"
#include <getopt.h>

#define MAX_SERVERS 64

double tput[MAX_SERVERS];

TestConfig config;

extern uint64_t one_second;

using namespace rdmaio;

// the functions that will be overwritten by the specified test implementation
void *run_server(void *arg); // server function

void print_microsecends(const struct timeval& start, const struct timeval& end, int node_id = -1){
  double microseconds = (end.tv_sec - start.tv_sec)*1000 +
    (double) (end.tv_usec - start.tv_usec) / 1000;
  printf("%f ms %d\n", microseconds, node_id);
}

int main(int argc, char *argv[]) {

  int num_threads(1);
  uint64_t memory_size(1);
  struct timeval start, end;
  
  ibv_wr_opcode opcode;
  static struct option opts[] = {
    { "num-threads",    1, NULL, 't' },
    { "memory",     1, NULL, 'm' },
    { 0 }
  };

  /* Parswe and check arguments */
  while(1) {
    int c = getopt_long(argc, argv, "t:m:", opts, NULL);
    if(c == -1) {
      break;
    }
    switch (c) {
    case 't':
      num_threads = atoi(optarg);
      break;
    case 'm':
      memory_size = atoi(optarg); // GB memory shall register
      break;
    default:
      printf("Invalid argument %d\n", c);
      assert(false);
    }
  }

  uint64_t G = 1024 * 1024 * 1024;
  uint64_t total_memory = memory_size * G;
  struct thread_config *configs = (struct thread_config*)malloc(num_threads * sizeof(struct thread_config));
  pthread_t *threads = (pthread_t*)malloc(num_threads * sizeof(pthread_t));

  printf("main: Using %d threads, user buf size %lu\n", num_threads,total_memory);
  
  std::vector<std::string> network;
  network.push_back("127.0.0.1");

  // init RDMA config

  RdmaCtrl *cm = new RdmaCtrl(0,network,8888,0);
  gettimeofday(&start, NULL);
  char *buffer = (char *)malloc(total_memory);
  gettimeofday(&end, NULL);
  print_microsecends(start,end);
  assert(buffer != NULL);
  cm->set_connect_mr(buffer,total_memory); // NULL, just using the default allocation strategy
  // cm->start_server();
  for(int i = 0; i < num_threads; i++) {
  printf("threads %d %d\n", num_threads, i);
      configs[i].node_id = 0;
      configs[i].num_threads = num_threads;
      configs[i].id = i;
      configs[i].cm = cm;
      pthread_create(&threads[i], NULL, run_server, &configs[i]);
  }
  // cm->start_server();
  for(int i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }
  printf("finish\n");
  return 0;
}


void *run_server(void *arg) {

  struct timeval start, end;
  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id; /* Global ID of this server thread */
  printf("thread: %d\n", thread_id);
  // create qps
  RdmaCtrl *cm = configs.cm;
  // choose device id
  char *registered_buf = (char *)(cm->conn_buf_);

  // modify the buffer based on the thread_id and node id
  // *((uint64_t *)(registered_buf + sizeof(uint64_t) * thread_id)) = _QP_ENCODE_ID(configs.node_id,
  //                                                                                thread_id);
  
  cm->thread_local_init();
  cm->open_device(0);
  gettimeofday(&start, NULL);
  cm->register_connect_mr(0);
  gettimeofday(&end, NULL);
  print_microsecends(start,end,thread_id);

  // gettimeofday(&start, NULL);
  // free(buffer);
  // gettimeofday(&end, NULL);
  // print_microsecends(start,end);

  // bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);
}
