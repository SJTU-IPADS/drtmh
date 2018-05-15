#ifndef NOCC_DB_TX_LAYER_HANDLER
#define NOCC_DB_TX_LAYER_HANDLER

#include "all.h"
#include "./db/config.h"

#include "db/remote_set.h"
#include "db/db_logger.h"

#include <stddef.h>
#include <stdint.h>

#define PROFILE_INIT()  uint64_t p_start,p_generate,p_execute,p_commit;
#define PROFILE_START() p_start = rdtsc();
#define PROFILE_GEN_END() p_generate = rdtsc() - p_start;
#define PROFILE_EXE_END() p_execute = rdtsc() - p_start;
#define PROFILE_COMMIT_END() p_commit = rdtsc() - p_start;
#define PROFILE_SUMMARY(list,idx)   list[idx].count += 1;	\
  list[idx].generate_time += p_generate;                    \
  list[idx].execution_time += p_execute;                    \
  list[idx].commit_time += p_commit;                        \

/* 16 bit mac | 6 bit thread | 10 bit cor_id  */
#define ENCODE_LOCK_CONTENT(mac,tid,cor_id) ( ((mac) << 16) | ((tid) << 10) | (cor_id) ) 
#define DECODE_LOCK_MAC(lock) ( (lock) >> 16)
#define DECODE_LOCK_TID(lock) (((lock) & 0xffff ) >> 10)
#define DECODE_LOCK_CID(lock) ( (lock) & 0x3f)

#define META_SIZE 16

namespace nocc {
  namespace db {

    struct Profile {
      uint64_t remote_servers;
      double rw_size;
      uint64_t count; // total TX counted

    Profile():
      remote_servers(0), rw_size(0),count(0)
      { }

      void process_rw(double size) {
        rw_size += size;
        count += 1;
      }
      double report() {
        return rw_size / count;
      }
    };

    struct TXProfile {

      int abortRemoteLock;
      int abortLocalLock;
      int abortLocalValidate;
      int abortRemoteValidate;
      int abortTotal;
      uint64_t abortKey;

      void reset() {
        abortRemoteLock = 0;
        abortLocalLock = 0;
        abortLocalValidate = 0;
        abortRemoteValidate = 0;
        abortTotal = 0;
      }

      void print() {
        fprintf(stdout,"check abort RemoteLock: %d || LocalLock: %d, total %d, abort key %lu\n",
                abortRemoteLock,abortLocalLock,abortTotal,abortKey);
      }

    };

    struct TXBreakdownProfile {
      uint64_t execution_time;
      uint64_t commit_time;
      uint64_t generate_time;
      uint64_t count;
    };

    /* Each transaction layer implementation shall provide read write and commit operations */
    class TXHandler {

      /* A trick to let pure virtual function to have default argument */
    protected:
      virtual void _begin(DBLogger *db_logger, TXProfile *p ) = 0;
    public:
      
      RemoteSet *remoteset;
      uint     cor_id_;
    TXHandler(int c_id) : cor_id_(c_id) { }
    
      void begin(DBLogger *db_logger = NULL, TXProfile *p = NULL) {
        return _begin(db_logger, p);
      }

      virtual void local_ro_begin() = 0;
      virtual bool end(yield_func_t &yield) = 0; /* like [commit] function  */
      virtual bool end_fasst(yield_func_t &yield) { assert(false); }

      // virtual bool end_ro() = 0; /* like [commit] function  */
      virtual void abort() = 0;
      virtual uint64_t get_timestamp() { return 0;}

      /* Get methods */
      virtual uint64_t get(int tableid,uint64_t key,char **val,int len) = 0;
      virtual uint64_t get_ro(int tableid,uint64_t key,char *val,yield_func_t &yield) = 0;
      virtual uint64_t get_ro_versioned(int tableid,uint64_t key,char *val,uint64_t version,yield_func_t &yield) = 0;

      /* Modifiers */
      virtual void write(int tableid,uint64_t key,char *val,int len) = 0 ;
      virtual void write() = 0;  /* promote the last *get* object to write*/

      virtual void insert(int tableid,uint64_t key,char *val,int len) = 0;
      virtual void insert_index(int tableid,uint64_t key,char *val)   = 0;
      virtual void delete_(int tableid,uint64_t key) = 0;
      virtual void delete_index(int tableid,uint64_t key) = 0;
      virtual void delete_by_node(int tableid, char *node) = 0;

      /* remote operators */
      virtual int  add_to_remote_set(int tableid,uint64_t key,int pid) = 0; // for short key
      virtual int  add_to_remote_set(int tableid,uint64_t key,int pid, yield_func_t &yield) {
        return add_to_remote_set(tableid,key,pid); //the default will not yield
      }
      virtual int  add_to_remote_set(int tableid,uint64_t *key,int klen,int pid) = 0; // for long key
      virtual int  add_to_remote_set_imm(int tableid,uint64_t key,int pid) {
        return remoteset->add_imm(REQ_READ,pid,tableid,key);} // no batching

      virtual int  remote_read_idx(int tableid,uint64_t *key,int klen,int pid) = 0;

      virtual int  remote_insert(int tableid,uint64_t *key, int klen,int pid) = 0;
      virtual int  remote_insert_idx(int tableid,uint64_t *key,int klen,int pid) = 0;

      virtual void remote_write(int idx,char *val,int len) = 0;
      virtual void remote_write(int tableid,uint64_t key,char *val,int len) = 0;

      /* called after remote value has been fetched */
      virtual uint64_t get_cached(int tableid,uint64_t key,char **val) = 0;
      virtual uint64_t get_cached(int r_idx,char **val) = 0;

      /* do remote reads can be seen as a combined version of do_remote_reads() + get_remote_results */
      virtual void do_remote_reads(yield_func_t &yield) = 0;
      virtual int  do_remote_reads() = 0;
      virtual void get_remote_results(int) = 0;
      virtual void get_imm_res(int idx,char **ptr,int size) { remoteset->get_result_imm(idx,ptr,size);}

      // some report things
      virtual int report_rw_set() {  return 0;}
      virtual void report() { return; } // report benchmark statics

      /* some minor statics */
      uint64_t     nreadro_locked;
      uint64_t     timestamp;
    };

    /* Each transaction layer implementation shall provide scan operations */
    class TXIterator {
    public:
      virtual uint64_t Key() = 0;
      virtual bool     Valid() = 0;
      virtual char    *Value() = 0;
      virtual char    *Node() = 0;
      virtual void     Next() = 0;
      virtual void     Seek(uint64_t key) = 0;
      virtual void     SeekToFirst() = 0;
      virtual void     SeekToLast()  = 0;
    };

  };
};

#endif
