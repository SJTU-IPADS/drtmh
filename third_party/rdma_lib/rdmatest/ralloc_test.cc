/* The file is used to test whether RDMA malloc is correct */

#include "ralloc.h"
//#include "rdmaio.h"

#include <stdint.h>
#include <vector>
#include <string>

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#define THREAD 10

std::vector<char *> pointer_buffer;
uint64_t buffer_size = 1024 * 1024 * 1024;
//RDMAQueues *rdma;
char *buffer = NULL;

void *thread_body (void *);

int main() {
  fprintf(stdout,"here\n");
#if 1
  buffer = (char *)malloc(buffer_size);
  fprintf(stdout,"malloc done\n");
  malloc(32);
  for(uint i = 0;i < 1;++i) {
    calloc(32,1);
  }
  fprintf(stdout,"malloc done\n");
  
  int port = 5555;
  std::vector<std::string> netDef;
#endif  
#if 0  
  netDef.push_back("10.0.0.103");
  //  rdma = bootstrapRDMA(0,port,netDef,THREAD,buffer,buffer_size);  
#if 1
  uint64_t real = RInit(buffer,buffer_size);
  assert(real != 0);
  fprintf(stdout,"r init done, actually allocated %lu size\n",real);
  /* Each thread must call one init */
#endif  
  RThreadLocalInit();
  fprintf(stdout,"r thread local init done\n");

  /* Basic function tests */

  char *localBuf = (char *)Rmalloc(1024);
  fprintf(stdout,"r malloc done\n");
  
  for(int i = 0;i < THREAD;++i) {
    int size = rand() % 100 + 50;
    char *lb = (char *)Rmalloc( size);
    uint64_t offset = lb - buffer;
    fprintf(stdout,"offset %lu, %d\n",offset,size);   
    assert(offset < buffer_size);
    ///    assert(rdma->read(_QP_ENCODE_ID(0,i),localBuf,offset,size) == RDMAQueues::IO_SUCC);    
    pointer_buffer.push_back(lb);
  }
  
#if 0
  fprintf(stdout,"strss test\n");  
  uint64_t size = 1;
  for(int i = 0;i < 100;++i) {
    size *= 2;
    char *xx = (char *)Rmalloc(size);
    if(xx == NULL) {
      fprintf(stdout,"failed allocation size %lu\n",size);
      break;
    }
    pointer_buffer.push_back(xx);
  }
#endif
  
  for(int i = THREAD;i < pointer_buffer.size();++i)
    Rfree((void *)pointer_buffer[i]);

  
  /* Basic function tests done */

  /* test multi-threading */

  pthread_t *ids = new pthread_t[THREAD];
  for(int i = 0;i < THREAD;++i) {

    pthread_create( ids + i,NULL,thread_body,(void *)i);
    //    pthread_create( ids + i,NULL,thread_body,(void *)(&(confs[i])));    
  }  

  for(int i = 0;i  < THREAD ; ++i) {
    pthread_join(ids[i],NULL);
  }
  fprintf(stdout,"done\n");
  
#endif
  return 0;
}

void *thread_body(void *arg) {
  int id = (uint64_t )(arg);
  fprintf(stdout,"thread %d started!\n",id);

  /* If u are intend to use Ralloc, then this shall be call at first */
  RThreadLocalInit();
  
  /* Test remote free */
  fprintf(stdout,"remote free test %d\n",id);
  Rfree((void *)(pointer_buffer[id]));

  std::vector<void *> r_pointers;
  std::vector<void *> l_pointers;
  
  char *localBuf = (char *)Rmalloc(1024);
  
  for(int i = 0;i < 1000;++i) {
    int size = rand() % 100 + 50;
    char *lb = (char *)Rmalloc( size);
    assert(lb != NULL);
    uint64_t offset = lb - buffer;
    //    assert(offset < buffer_size);
    //    assert(rdma->read(_QP_ENCODE_ID(0,id),localBuf,offset,size) == RDMAQueues::IO_SUCC);
    r_pointers.push_back(lb);

    char *ll = (char *)malloc(size);
    assert(ll != NULL);
    l_pointers.push_back(ll);

    if( i % 200 == 0) {
      for(int j = 0;j < r_pointers.size();++j)
	Rfree(r_pointers[j]);
      r_pointers.clear();
      for(int j = 0;j < l_pointers.size();++j)
	free(l_pointers[j]);
      l_pointers.clear();
    }
  }
  // further cleaning
  for(int j = 0; j < r_pointers.size();++j) {
    Rfree(r_pointers[j]);
  }
  for(int j = 0;j < l_pointers.size();++j) {
    free(l_pointers[j]);
  }
}
