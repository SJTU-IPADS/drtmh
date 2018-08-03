#include "ralloc.h"
#include "rdmaio.h"

#include "all.h"

#include <getopt.h>
#include <dlfcn.h>


double tput[MAX_SERVERS];
TestConfig config;




// the functions that will be overwritten by the specified test implementation
void *run_server(void *arg); // server function
void *run_client(void *arg); // client function

extern uint64_t one_second;

using namespace rdmaio;

int main(int argc, char *argv[]) {

  int num_threads, is_client, node_id;
  bool enable_single_mr;

  ibv_wr_opcode opcode;
  static struct option opts[] = {
    { "num-threads",    1, NULL, 't' },
    { "node-id",     1, NULL, 'n' },
    { 0 }
  };

  /* Parswe and check arguments */
  while(1) {
    int c = getopt_long(argc, argv, "t:n:", opts, NULL);
    if(c == -1) {
      break;
    }
    switch (c) {
    case 't':
      num_threads = atoi(optarg);
      break;
    case 'n':
      node_id = atoi(optarg);
      break;
    default:
      printf("Invalid argument %d\n", c);
      assert(false);
    }
  }

  config.open_file("config.xml");

  config.read_iter_num();
  config.read_network();
  config.read_port();
  config.read_op();
  config.read_req_length();
  config.read_batch_size();
  config.read_user_bufsize();
  config.read_rc_per_thread();
  config.read_uc_per_thread();
  config.read_ud_per_thread();
  config.read_enable_single_mr();
  // is_client = node_id != 1 ? 1 : 0;
  is_client = 1;
  enable_single_mr = config.enable_single_mr_ != 0;

  // is_client = 1;
  switch(config.op_){
    case 0:
      opcode = IBV_WR_RDMA_WRITE;
      break;
    case 1:
      opcode = IBV_WR_RDMA_READ;
      break;  
  }

  assert(num_threads >= 1);
  assert(1 <= config.batch_size_);
  assert(node_id >= 0);
  assert(config.req_length_ >= 0);

  for(int i = 0; i < MAX_SERVERS; i++) {
    tput[i] = 0;
  }

  printf("main: Using %d threads, user buf size %lu\n", num_threads,config.user_bufsize_);
  struct thread_config *configs = (struct thread_config*)malloc(num_threads * sizeof(struct thread_config));
  pthread_t *threads = (pthread_t*)malloc(num_threads * sizeof(pthread_t));

  // init RDMA config
  RdmaCtrl *cm = new RdmaCtrl(node_id,config.network_,config.port_,false);
  // char *buffer = (char *)malloc(config.user_bufsize_);
  char *buffer = (char *)malloc_huge_pages(config.user_bufsize_,HUGE_PAGE_SIZE,1);

  if(config.rc_per_thread_ > 0 || config.uc_per_thread_ > 0 || config.ud_per_thread_ > 0){
    cm->set_connect_mr(buffer,config.user_bufsize_); // NULL, just using the default allocation strategy
    memset((void *) cm->conn_buf_, 0, config.user_bufsize_);
    printf("buffer size:%lu\n",config.user_bufsize_);
  }

  cm->start_server();

  // init ralloc
  // RInit((char *)(cm->conn_buf_),config.user_bufsize_);

  // check the time
  {
    char buffer[80];

    time_t rawtime;
    time (&rawtime);

    tm *timeinfo = localtime(&rawtime);

    strftime(buffer,sizeof(buffer),"Prepared to started, at %d-%m-%Y %I:%M:%S",timeinfo);
    std::string str(buffer);
    std::cout<<str<<endl;
  }


  for(int i = 0; i < num_threads; i++) {
    configs[i].node_id = node_id;
    configs[i].num_threads = num_threads;
    configs[i].id = i;
    configs[i].opcode = opcode;
    configs[i].cm = cm;
    if(is_client) {
      pthread_create(&threads[i], NULL, run_client, &configs[i]);
    } else {
      pthread_create(&threads[i], NULL, run_server, &configs[i]);
    }
  }
  fprintf(stdout,"Wait for joining... \n");

  for(int i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }
  fprintf(stdout,"Benchmark done\n");
  return 0;
}
