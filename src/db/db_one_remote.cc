#include "db_one_remote.h"
#include "tx_config.h"

#include "ralloc.h"                    // for Rmalloc
#include "framework/bench_worker.hpp"  // for worker->indirect_yield

#include "util/util.h"

#define MAX_ELEMS 32   // max elems the remote item fetch contains
#define DOORBELL 1

using namespace rdmaio;

namespace nocc {

  extern __thread oltp::BenchWorker* worker;

  namespace db {

#define MAX_VAL_SZ (1024 - sizeof(RRWSet::Meta) - sizeof(RRWSet::Meta)) // max value per record
    // VAL buffer format: | meta (read phase) | payload | meta (for validation) |

    RRWSet::RRWSet(rdmaio::RdmaCtrl *cm,RDMA_sched *sched,MemDB *db,int tid,int cid,int meta)
      :tid_(tid),cor_id_(cid),db_(db),meta_len_(meta),sched_(sched),
       elems_(0),read_num_(0)
    {
      assert(cm != NULL);

      // get qp vector for one-sided operations
      auto num_nodes = cm->get_num_nodes();
      for(uint i = 0;i < num_nodes;++i) {
        for(uint j = 0;j < QP_NUMS ; j++){
          rdmaio::Qp *qp = cm->get_rc_qp(tid_,i,j);
          assert(qp != NULL);
          qps_.push_back(qp);
        } 
      }
      memset(qp_idx_,0,sizeof(int)*16);

      // init local remote key-value cache
      kvs_ = new RemoteSetItem[MAX_ELEMS];
      for(uint i = 0;i < MAX_ELEMS;++i) {
        kvs_[i].pid = 0;
        kvs_[i].key = 0;
        kvs_[i].off = 0;
        kvs_[i].seq = 0;
        kvs_[i].val = (char *)Rmalloc(MAX_VAL_SZ + sizeof(Meta));
      }

      INIT_LAT_VARS(post);
    }

    int RRWSet::add(int pid,int tableid,uint64_t key,int len,yield_func_t &yield) {

      Qp* qp = get_qp(pid);
      // Qp* qp = qps_[pid];

      auto off = db_->stores_[tableid]->RemoteTraverse(key,qp,sched_,yield,kvs_[elems_].val);
      //auto off = db_->stores_[tableid]->RemoteTraverse(key,qp);

      // add to the pending qp list
      qp->rc_post_send(IBV_WR_RDMA_READ,kvs_[elems_].val,len,off,
                       IBV_SEND_SIGNALED,cor_id_);
      sched_->add_pending(cor_id_,qp);
      worker->indirect_yield(yield); // yield for waiting for NIC's completion

      // add the record to the read write set
      kvs_[elems_].pid     = pid;
      kvs_[elems_].key     = key;
      kvs_[elems_].off     = off;
      kvs_[elems_].len     = len;
      kvs_[elems_].tableid = tableid;
      kvs_[elems_].ro      = false;

      return elems_++;
    }

    int RRWSet::add(int pid,int tableid,uint64_t key,int len) {

      Qp* qp = get_qp(pid);

      // fetch the remote items using the mem store
      auto off = db_->stores_[tableid]->RemoteTraverse(key,qp,kvs_[elems_].val);

      // fetch the data record
      // FIXME!, we need to ensure that the value returned is consistent by checking \
      the checksums of each data record
      // it seems that it may cause higher abort rate
      //qp->rc_post_send(IBV_WR_RDMA_READ,kvs_[elems_].val,len,off,
      //IBV_SEND_SIGNALED,cor_id_);

      // add to the pending qp list
      sched_->add_pending(cor_id_,qp);

      // add the record to the read write set
      kvs_[elems_].pid     = pid;
      kvs_[elems_].key     = key;
      kvs_[elems_].off     = off;
      kvs_[elems_].len     = len;
      kvs_[elems_].tableid = tableid;
      kvs_[elems_].ro      = false;

      return elems_++;
    }

    bool RRWSet::validate_reads(yield_func_t &yield) {

      if(read_num_ > 0) {
        // send out read requests
        for(uint i = 0;i < elems_;++i) {

          if(!kvs_[i].ro) continue;

          // prepare targets
          auto pid = kvs_[i].pid;
          auto off = kvs_[i].off;
          auto val = kvs_[i].val;

          memcpy(&(kvs_[i].seq), kvs_[i].val + sizeof(uint64_t), sizeof(uint64_t));

          Qp* qp = get_qp(pid);
          qp->rc_post_send(IBV_WR_RDMA_READ,val + sizeof(uint64_t),sizeof(uint64_t),
                                  off + sizeof(uint64_t),IBV_SEND_SIGNALED,cor_id_);

          sched_->add_pending(cor_id_,qp);

        } // end iterating validation values

        worker->indirect_yield(yield); // yield for waiting for NIC's completion

        // parse the result
        for(uint i = 0;i < elems_;++i) {

          if(!kvs_[i].ro) continue;

          auto val = (char *)(kvs_[i].val);

          Meta *meta = (Meta *)(val);                             // meta before

          if(meta->seq != kvs_[i].seq) {
            return false;
          }

        } // end check results
      }   // end read validation using one-sided reads
      return true;
    }

    bool RRWSet::lock_remote(uint64_t lock_content, yield_func_t &yield) {
      
      bool res = true;

      for(uint i = 0;i < elems_;++i) {
        if(kvs_[i].ro) continue;

        auto pid = kvs_[i].pid;
        auto off = kvs_[i].off;
        auto val = kvs_[i].val;

        memcpy(&(kvs_[i].seq), kvs_[i].val + sizeof(uint64_t), sizeof(uint64_t));

        Qp* qp = get_qp(pid);

        // DZY: pay attention! if the number of coroutines is too high, and in TPCC, 
        //      the pending requests of send queue can run out of memory
        qp->rc_post_compare_and_swap(val,off,0, lock_content, 0,cor_id_);

        //qp->rc_post_send(IBV_WR_RDMA_READ,val + sizeof(uint64_t),sizeof(uint64_t),
        //          off + sizeof(uint64_t),IBV_SEND_SIGNALED,cor_id_);

        sched_->add_pending(cor_id_,qp);

      } // end iterating remote lock

      START(post);
      worker->indirect_yield(yield); // yield for waiting for NIC's completion
      END(post);

      // parse the result
      for(uint i = 0;i < elems_;++i) {

        if(kvs_[i].ro) continue;

        auto val = (char *)(kvs_[i].val);
        auto off = kvs_[i].off;

        Meta *meta = (Meta *)(val);

        // if not all locks are successfully set, release the locked ones
        if(meta->lock != 0 || meta->seq != kvs_[i].seq){
          res = false;
          break;
        }

      } // end check results

      return res;
    }

    void RRWSet::release_remote() {
      uint64_t free_lock = 0;

      for(uint i = 0;i < elems_;++i) {
        if(kvs_[i].ro) continue;

        auto pid = kvs_[i].pid;
        auto off = kvs_[i].off;
        auto val = kvs_[i].val;

        Qp* qp = get_qp(pid);

        if(*((uint64_t*)val) == 0) {
          qp->rc_post_send(IBV_WR_RDMA_WRITE,(char*)(&free_lock),sizeof(uint64_t),
          off,IBV_SEND_INLINE,cor_id_);
        }
      } // end iterating remote lock
    }


    int RRWSet::write_all_back(int meta_off,int data_off,char *buffer) {

      // FIXME!!: assume meta format:
      //{
      // uint64_t lock;
      // uint64_t seq;
      //}
      char *cur_ptr = buffer; // iterating pointer of the buffer

      for(uint i = 0;i < elems_;++i) {

        auto pid = kvs_[i].pid;
        auto off = kvs_[i].off;
        auto val = kvs_[i].val;
        auto len = kvs_[i].len;

        // set the data
        memcpy(cur_ptr,val,len + meta_len_);

        // set the meta
        Meta *meta = (Meta *)(cur_ptr + meta_off);
        meta->lock = 0;
        meta->seq += 2;

        // write the data payload
        Qp* qp = qps_[pid];
        int  flag = 0;
        //if(qp->first_send()) flag = IBV_SEND_SIGNALED;
        //if(qp->need_poll())  qp->poll_completion();

        qp->rc_post_send(IBV_WR_RDMA_WRITE,cur_ptr + data_off,len,off + data_off, \
                                flag,cor_id_); // FIXME: is it ok to not signaled this?

        // write the meta data, including release the locks and other things
        qp->rc_post_send(IBV_WR_RDMA_WRITE,cur_ptr + meta_off,meta_len_,off + meta_off, \
                                IBV_SEND_INLINE,cor_id_);
        cur_ptr = cur_ptr + meta_len_ + len;
      }
    }

  }; // namespace db
};   // namespace nocc
