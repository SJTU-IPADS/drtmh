#include "occ.h"

namespace nocc {

namespace rtx  {

template <int tableid,typename V>
inline __attribute__((always_inline))
int OCC::read(int pid,uint64_t key,yield_func_t &yield) {
  if(pid == node_id_)
    return local_read(tableid,key,sizeof(V),yield);
  else {
    // remote case
    return remote_read(pid,tableid,key,sizeof(V),yield);
  }
}

template <int tableid,typename V>
inline __attribute__((always_inline))
int OCC::pending_read(int pid,uint64_t key,yield_func_t &yield) {
  if(pid == node_id_)
    return local_read(tableid,key,sizeof(V),yield);
  else
    return pending_remote_read(pid,tableid,key,sizeof(V),yield);
}

template <int tableid,typename V>
inline __attribute__((always_inline))
int OCC::insert(int pid,uint64_t key,V *val,yield_func_t &yield) {
  if(pid == node_id_)
    return local_insert(tableid,key,(char *)val,sizeof(V),yield);
  else {
    return remote_insert(pid,tableid,key,sizeof(V),yield);
  }
  return -1;
}


template <int tableid,typename V>
inline __attribute__((always_inline))
int OCC::add_to_write(int pid,uint64_t key,yield_func_t &yield) {
  if(pid == node_id_){
    assert(false); // not implemented
  }
  else {
    // remote case
    return add_batch_write(tableid,key,pid,sizeof(V));
  }
  return -1;
}

inline __attribute__((always_inline))
int OCC::add_to_write(int idx) {
  assert(idx >= 0 && idx < read_set_.size());
  write_set_.emplace_back(read_set_[idx]);

  // eliminate read-set
  // FIXME: is it necessary to use std::swap to avoid memcpy?
  read_set_.erase(read_set_.begin() + idx);
  return write_set_.size() - 1;
}

inline __attribute__((always_inline))
int OCC::add_to_write() {
  return add_to_write(read_set_.size() - 1);
}

template <typename V>
inline __attribute__((always_inline))
V *OCC::get_readset(int idx,yield_func_t &yield) {
  assert(idx < read_set_.size());
  ASSERT(sizeof(V) == read_set_[idx].len) <<
      "excepted size " << (int)(read_set_[idx].len)  << " for table " << (int)(read_set_[idx].tableid) << "; idx " << idx;

  if(read_set_[idx].data_ptr == NULL
     && read_set_[idx].pid != node_id_) {

    // do actual reads here
    auto replies = send_batch_read();
    assert(replies > 0);
    worker_->indirect_yield(yield);

    parse_batch_result(replies);
    assert(read_set_[idx].data_ptr != NULL);
    start_batch_rpc_op(read_batch_helper_);
  }
  return (V *)(read_set_[idx].data_ptr);
}

template <typename V>
inline __attribute__((always_inline))
V *OCC::get_writeset(int idx,yield_func_t &yield) {
  assert(idx < write_set_.size());
  return (V *)write_set_[idx].data_ptr;
}


inline __attribute__((always_inline))
void OCC::gc_readset() {
  for(auto it = read_set_.begin();it != read_set_.end();++it) {
    //if(it->pid == node_id_)
    free((*it).data_ptr);
  }
}

inline __attribute__((always_inline))
void OCC::gc_writeset() {
  for(auto it = write_set_.begin();it != write_set_.end();++it) {
    //if(it->pid == node_id_)
    free((*it).data_ptr);
  }
}

} // namespace rtx

} // namespace nocc
