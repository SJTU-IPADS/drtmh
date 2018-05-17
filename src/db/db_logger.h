#ifndef NOCC_DB_DBLOGGER_H_
#define NOCC_DB_DBLOGGER_H_

#include "all.h"
#include "global_config.h"
#include "tx_config.h"

#include "core/routine.h"
#include "core/rdma_sched.h"
#include "core/rrpc.h"

#include "framework/view_manager.h"
#include "framework/log_cleaner.h"

#include "util/temp_log.h"
#include "./config.h"
#include "rdmaio.h"



//                        logger memory structure
//  |---------------------------node_area----------------------------|
//  ||---------thread_area----------|                                |
//  |||-META-||---BUF---||-PADDING-||                                |
//  {([      ][         ][         ])([      ][         ][         ])} {....}

#define RPC_LOGGING 10
#define RPC_LOGGING_COMMIT 11

using namespace rdmaio;

namespace nocc  {

  using namespace oltp;

  namespace db{

    const uint32_t THREAD_AREA_SIZE = 16 * 1024 * 1024;
    const uint32_t THREAD_META_SIZE = sizeof(uint64_t) * MAX_SERVERS;
    const uint32_t THREAD_PADDING_SIZE = 32 * 1024;
    const uint32_t THREAD_BUF_SIZE = THREAD_AREA_SIZE - THREAD_META_SIZE - THREAD_PADDING_SIZE;

    const uint32_t LOG_HEADER_MAGIC_NUM = 73;
    const uint32_t LOG_TAILER_MAGIC_NUM = 74;
    const uint32_t LOG_ACK_NUM = 0x888888;

    // To avoid ring buffer bug when logger tailer == header
    const uint32_t LOG_HOLE = 1024;

    struct TXHeader {
      uint64_t  magic_num;
      uint64_t  global_seq;
    };
    struct TXTailer {
      uint64_t  magic_num;
      uint64_t  seq;
    };
    struct EntryMeta {
      int32_t table_id;
      uint32_t size;
      uint64_t key;
      uint64_t seq;
    };

    class DBLogger {
    public:
      enum LogStatus {
        LOG_SUCC = 0,
        LOG_NOT_COMPLETE,
        LOG_NOT_USE,
        LOG_TIMEOUT,
        LOG_ERR
      };
#if TX_LOG_STYLE > 0
      struct RequestHeader{
        uint32_t length;
        uint64_t offsets[MAX_SERVERS];
      };
      // TODO: currently, no checking whether ack is right
      struct ReplyHeader{
        uint32_t ack;
      };
#endif

      //DZY: use set_base_offset before constructor!!
#if  TX_LOG_STYLE == 0
      DBLogger(int tid,RdmaCtrl *rdma,View *v, RDMA_sched* rdma_sched);
#elif TX_LOG_STYLE == 1
      DBLogger(int tid,RdmaCtrl *rdma,View *v, RRpc *rpc_handler);
#elif TX_LOG_STYLE == 2
      DBLogger(int tid,RdmaCtrl *rdma,View *v, RDMA_sched* rdma_sched, RRpc *rpc_handler);
#endif
      ~DBLogger();

      void thread_local_init();

      //global_seq == 0 means each entry in the log needs a seq corresponding
      void log_begin(uint cor_id, uint64_t global_seq = 0);
      char* get_log_entry(uint cor_id, int tableid, uint64_t key, uint32_t size, int partition_id = -1);
      void close_entry(uint cor_id, uint64_t seq = 0);
      int log_backups(uint cor_id, uint64_t seq = 0);
      int log_setup(uint cor_id);
      int log_backups_ack(uint cor_id);
      int log_end(uint cor_id);
      int log_abort(uint cor_id);

#if TX_LOG_STYLE == 1
      void logging_handler(int id,int cid,char *msg,void *arg);
#endif
#if TX_LOG_STYLE > 0
      void logging_commit_handler(int id,int cid,char *msg,void *arg);
#endif


      // print functions, for debugging
      void print_total_mem();
      void print_node_area_mem(int node_id);
      void print_thread_area_mem(int node_id, int thread_id);
      char* print_log(char* ptr);
      char* print_log_header(char *ptr);
      char* print_log_entry(char *ptr);
      char* print_log_tailer(char *ptr);

      // global constants
      static LogCleaner *log_cleaner_;
      static char* check_log_completion(volatile char* ptr, uint64_t *msg_size = NULL);
      static void clean_log(char* log_ptr, char* tailer_ptr);
      static void set_log_cleaner(LogCleaner *log_cleaner){
        assert(log_cleaner_ == NULL);
        log_cleaner_ = log_cleaner;
        assert(log_cleaner_ != NULL);
      }

      static uint64_t base_offset_;  // base offset in the registered RDMA
      static void set_base_offset(uint64_t base_offset){
        if(base_offset_ == 0)
          base_offset_ = base_offset;
        else if (base_offset_ != base_offset)
          assert(false);
      }
      static uint64_t get_base_offset(){
        assert(base_offset_ != 0);
        return base_offset_;
      }
      static uint64_t get_memory_size(size_t nthreads, int num_nodes){
        return nthreads * THREAD_AREA_SIZE * num_nodes;
      }

    private:
      inline uint64_t translate_offset(uint64_t offset){
        assert(offset < THREAD_BUF_SIZE + THREAD_PADDING_SIZE);
        return offset + translate_offset_;
      }

    private:

      RdmaCtrl *rdma_;
#if TX_LOG_STYLE == 0

      RDMA_sched * rdma_sched_;

#elif TX_LOG_STYLE == 1

      RRpc *rpc_handler_;

#elif TX_LOG_STYLE == 2

      RDMA_sched * rdma_sched_;
      RRpc *rpc_handler_;

#endif

      Qp* get_qp(int pid){
        int idx = (qp_idx_[pid]++) % QP_NUMS;
        return qp_vec_[pid*QP_NUMS + idx];
      }
      int qp_idx_[16];
      std::vector<Qp *> qp_vec_;
      View view_;

      int num_nodes_;
      int thread_id_;

      TempLog **temp_logs_;


      // used for ring buffer when backup thread cleaner is present
      uint64_t node_area_size_;
      uint64_t translate_offset_;

      char *base_ptr_;        // ptr to start of logger memory
      char *base_local_buf_ptr_;   // ptr to start of base thread buf area(node_id = 0, thread_id = thread_id_)
      char *local_meta_ptr_;  // ptr to start of local thread meta area

      // the remaining size for each machine
      int      *remain_sizes_;
      // the offsets of remote machine ring buffer
      uint64_t *remote_offsets_;

    };

  } // namespace db
} //namespace nocc

#endif //NOCC_DB_DBLOGGER_H_
