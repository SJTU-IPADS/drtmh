#include "ralloc.h"
#include "rdmaio.h"
#include "ring_msg.h"

#include "all.h"

#include "TestConfigXml.h"

#define MAX_SERVERS 64

extern double tput[MAX_SERVERS];
extern TestConfig config;

uint64_t one_second = 0;

int stick_this_thread_to_core(int);

using namespace ::rdmaio;
using namespace ::rdmaio::ringmsg;


void *run_server(void *arg) {

  // general init
  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */

  // create qps
  RdmaCtrl *cm = configs.cm;
  // choose device id
  int dev_id = 0;
  int port_idx = 1;

  auto qp_vec = bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);

  // init message
  uint64_t total = 4 * 1024;
  uint64_t padding = 128;
  uint64_t ringsz = total - padding - MSG_META_SZ;

  // emulate 2 threads
  RingMessage *msg_handler[2];
  msg_handler[0] = new RingMessage(ringsz,padding,0,cm,(char *)(cm->conn_buf_));
  msg_handler[1] = new RingMessage(ringsz,padding,1,cm,(char *)(cm->conn_buf_));


  fprintf(stdout,"receiver start ...\n");
  int msg_count = 0;
  while(1) {

    char buf[1024];
    if(msg_handler[0]->try_recv_from(0,buf)) {
      //      fprintf(stdout,"recv from 0,0, %s\n",buf);
      msg_count += 1;
      fprintf(stdout,"msg %d %s\n",msg_count,buf);
    }

    if(msg_handler[1]->try_recv_from(0,buf)) {
      //      fprintf(stdout,"recv from 0,1, %s\n",buf);
      msg_count += 1;
      fprintf(stdout,"msg %d %s\n",msg_count,buf);
    }
  }



}


void *run_client(void *arg) {

  int mids[2]; // send to 2 servers

  // general init
  struct thread_config configs = *(struct thread_config *) arg;
  int thread_id = configs.id;	/* Global ID of this server thread */

  // create qps
  RdmaCtrl *cm = configs.cm;
  // choose device id
  int dev_id = 0;
  int port_idx = 1;

  auto qp_vec = bootstrap_thread_qps(cm,thread_id,dev_id,port_idx);

  // init ralloc
  RThreadLocalInit();
  char *msg_buf1 = (char *)Rmalloc(1024);

  // init message
  uint64_t total = 4 * 1024;
  uint64_t padding = 128;
  uint64_t ringsz = total - padding - MSG_META_SZ;

  // emulate 2 threads
  RingMessage *msg_handler[2];
  msg_handler[0] = new RingMessage(ringsz,padding,0,cm,(char *)(cm->conn_buf_));
  msg_handler[1] = new RingMessage(ringsz,padding,1,cm,(char *)(cm->conn_buf_));

  fprintf(stdout,"client: msg handler init done\n");

  *((uint64_t *)msg_buf1) = 80;
  *((uint64_t *)(msg_buf1 + sizeof(uint64_t) + 80)) = 80;
  for(uint j = 0; j < 10;++j) {
    mids[0] = 1;
    mids[1] = 2;
    msg_handler[0]->broadcast_to(mids, 2, msg_buf1, 80);
  }
  fprintf(stdout,"at client... first broadcast done\n");

  while(1) {

    int mid;
    int tid;
    char *buf = (char *)Rmalloc(1024);
    scanf ("%d",&tid);
    if(tid > 1) {
      int t = rand() % 2;
      int m = rand() % 2 + 1;
      fprintf(stdout,"send to %d\n",m);

      scanf ("%s",buf + sizeof(uint64_t));
      fprintf(stdout,"send to %d %d, :: %s\n",mid,tid,buf + sizeof(uint64_t));
      uint64_t slen = strlen(buf + sizeof(uint64_t));
      //uint64_t slen = 12;
      *((uint64_t *)buf) = slen + 1;
      *((uint64_t *)(buf + slen + 1 + sizeof(uint64_t))) = slen + 1;

      for(int i = 0;i < tid;++i) {
        msg_handler[t]->send_to(m,buf,slen + 1);
      }
      continue;
    }
    mids[0] = 1;
    mids[1] = 2;

    //    scanf ("%d",&mid);
    scanf ("%s",buf + sizeof(uint64_t));
    fprintf(stdout,"send to %d %d, :: %s\n",mid,tid,buf + sizeof(uint64_t));
    uint64_t slen = strlen(buf + sizeof(uint64_t));
    //uint64_t slen = 12;
    *((uint64_t *)buf) = slen + 1;
    *((uint64_t *)(buf + slen + 1 + sizeof(uint64_t))) = slen + 1;
    msg_handler[tid]->broadcast_to(mids, 2, buf, slen + 1);
  }

}
