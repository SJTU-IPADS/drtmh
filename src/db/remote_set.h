#ifndef NOCC_TX_REMOTE_SET
#define NOCC_TX_REMOTE_SET

#include "all.h"
#include "config.h"
#include "./db/config.h"
#include "db_statistics_helper.h"
#include "memstore/memstore.h"

#include "core/rrpc.h"

#include <stdint.h>

enum RPC_ID {
  RPC_READ = 1,
  RPC_LOCK,
  RPC_VALIDATE,
  RPC_RELEASE,
  RPC_COMMIT,
  RPC_LOCKED_READS, // FaSST style reads
  RPC_R_VALIDATE
};

enum REQ_TYPE {
  REQ_READ = 0,
  REQ_READ_IDX,
  REQ_INSERT,
  REQ_INSERT_IDX,
  REQ_RANGE_SEARCH,
  REQ_WRITE,
  REQ_READ_LOCK
};

namespace nocc {

  using namespace oltp;

  namespace db {

    class RemoteSet {
    public:
      // class related structs
#include "remote_set_structs.h"

      /* class methods  ******************************************************/

      RemoteSet(RRpc *rpc,int cid,int tid);

      // This call will read all value in parallal, assuming we know all the remote read set
      void do_reads(yield_func_t &yield);
      int  do_reads(int tx_id = 73);
      bool get_results(int);

      bool get_results_readlock(int); // get the result of read lock request
      bool get_result_imm(int idx, char **ptr,int size);          // get the reuslt of immediate read
      bool get_result_imm_batch(int start_idx,RemoteReqObj *reqs,int num);

      int  add(REQ_TYPE type,int pid,int8_t tableid,uint64_t key);

      /* return the index of the cached entries */
      int  add(REQ_TYPE type,int pid,int8_t tableid,uint64_t *key,int klen);
      int  add_imm(REQ_TYPE type,int pid,int8_t tableid,uint64_t key); // direct send reqs
      int  add_batch_imm(REQ_TYPE type,RemoteReqObj *reqs,int num);   // direct send reqs in a batch way

      /* num, num of items required to return */
      int  add_range(int pid,int8_t tableid,uint64_t *min,uint64_t *max,int klen,int num);

      void promote_to_write(int id,char *val,int len);

      void write(int8_t tableid, uint64_t key,  char *val,int len);
      /* this is used to encode some addition meta data in the message */
      char* get_meta_ptr()  {
        return ((char *)request_buf_) + sizeof(RequestHeader);
      }

      // These codes actually shall be refined later
      bool lock_remote(yield_func_t &yield);

      /* for fasst */
      int add_update_request(int pid,int8_t tableid,uint64_t key);
      int add_read_request(int pid,int8_t tableid,uint64_t key);

      bool validate_remote(yield_func_t &yield);

      void release_remote(yield_func_t &yield);

      void commit_remote(yield_func_t &yield);
      void commit_remote_naive(yield_func_t &yield);
      void clear(int meta_size = 0);
      void reset();

      void update_read_buf();
      void update_write_buf();

      void clear_for_reads();

      void set_lockpayload(uint64_t payload);
      int  get_broadcast_num() { return read_server_num_;}

      char *get_req_buf();

      /* helper functions */
      inline bool _check_res(int); /* check whether a validation or lock result is successfull */

      void report() {  REPORT(lock); }

      /* Class members *******************************************************/
      int max_length_;
      int elems_; // elems in the local cache
      uint16_t write_items_;
      uint16_t read_items_;
      RemoteSetItem *kvs_;

      RRpc *rpc_;

      char *request_buf_;
      char *request_buf_end_;

      char *lock_request_buf_;
      char *lock_request_buf_end_;

      char *write_back_request_buf_;
      char *write_back_request_buf_end_;

      /* used to receive objs reads */
      char *reply_buf_;
      char *reply_buf_end_;
      int   reply_buf_size_;

    public:
      // used to receive lock & validate results
      // this is exposed to users to allow more works
      char *reply_buf1_;
      uint64_t max_time_;
      int meta_size_;
      uint cor_id_;
      uint tid_;
      uint64_t count_;
      bool need_validate_;

    private:
      void print_write_server_list();
      std::set<int> server_set_;
      std::set<int> write_server_set_;
      int read_servers_[MAX_SERVER_TO_SENT];
      int read_server_num_;
      int write_servers_[MAX_SERVER_TO_SENT];
      int write_server_num_;

      // some statictics
      LAT_VARS(lock);
    };

  }
}
#endif
