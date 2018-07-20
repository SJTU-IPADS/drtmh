#ifndef NOCC_TX_FORK_SET
#define NOCC_TX_FORK_SET

/* This file is used to helpe transactions fork many requests to many servers */
#include "all.h"
#include "memstore/memstore.h"

#include "core/rrpc.h"

namespace nocc {

using namespace oltp; // framework

namespace db {

class ForkSet {
  public:
    ForkSet(RRpc *rpc,int cid = 0) ;
    ~ForkSet();
    /* start some initlization work*/
    void  reset();
    void  clear();
    char* do_fork(int sizeof_header );
    void  do_fork();

    char* add(int pid,int sizeof_payload);
    void  add(int pid);
    int   fork(int id,int type = RRpc::Y_REQ);
    int   fork(int id,char *val,int size,int type = RRpc::Y_REQ);
    char *get_reply() { return reply_buf_;}
    int   get_server_num() { return server_num_;}

    char  *reply_buf_;
  private:
    RRpc *rpc_;

    char  *msg_buf_start_;
    char  *msg_buf_end_;

    std::set<int> server_set_;
    int server_lists_[MAX_SERVER_TO_SENT];
    int server_num_;
    int cor_id_;
};
};
};
#endif
