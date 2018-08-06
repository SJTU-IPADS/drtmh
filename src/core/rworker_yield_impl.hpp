namespace nocc {
namespace oltp {

// class nocc_worker
inline ALWAYS_INLINE
void RWorker::indirect_yield(yield_func_t &yield) {
  // make a simple check to avoid yield, if possible
  if(unlikely(rdma_sched_->pending_counts_[cor_id_] == 0
              && !rpc_->has_pending_reqs(cor_id_))) {
    return;
  }
  indirect_must_yield(yield);
}

inline ALWAYS_INLINE
void RWorker::indirect_must_yield(yield_func_t &yield) {

  int next = routine_meta_->next_->id_;
  cor_id_  = next;
  auto cur = routine_meta_;
  routine_meta_ = cur->next_;
  change_ctx(cor_id_);
  cur->yield_from_routine_list(yield);

  change_ctx(cor_id_);
}


inline ALWAYS_INLINE
void RWorker::yield_next(yield_func_t &yield) {
  // yield to the next routine
  int next = routine_meta_->next_->id_;
  routine_meta_ = routine_meta_->next_;
  cor_id_  = next;
  change_ctx(cor_id_);

  routine_meta_->yield_to(yield);

  change_ctx(cor_id_);
  assert(cor_id_ == routine_meta_->id_);
}

// end class nocc_worker

} //
} // namespace nocc
