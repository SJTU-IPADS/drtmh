#ifndef NOCC_DB_ROUTINE_H
#define NOCC_DB_ROUTINE_H

#include "all.h"

#include <queue>          // std::queue

namespace nocc {

namespace oltp {

// one-shot routine's initilzation ,actually uses worker
class RWorker;

const int MAX_ONE_SHOT_CALLBACK = 8;
struct RoutineMeta;

struct one_shot_req {
  char *msg;
  int   from_mac; // mac id which send the requests
  int   from_routine; // routine id which send the req
  int   req_id;   // the req id
  one_shot_req(char *msg,int fm,int fc,int req) :
      msg(msg),from_mac(fm),from_routine(fc),req_id(req) { }

  one_shot_req() {}
};

typedef std::function<void(yield_func_t &,int,int,char *,void *)> one_shot_func_t;

void one_shot_func(yield_func_t &yield,RoutineMeta *meta);

// expose some of the data structures
extern __thread one_shot_func_t *one_shot_callbacks;
extern __thread RoutineMeta *next_routine_array;
extern __thread RoutineMeta *routine_tailer;
extern __thread RoutineMeta *routine_header;

extern __thread std::queue<RoutineMeta *> *ro_pool;

// functions exposed to the upper layer
// -----------------------------------
struct RoutineMeta {

  RoutineMeta() {active_ = false;  id_ = 0;}

  static void register_callback(one_shot_func_t callback,int id);

  static void thread_local_init(int num_ros,int coroutines,coroutine_func_t *,RWorker *w);

  RoutineMeta *prev_;
  RoutineMeta *next_;
  int  id_;
  coroutine_func_t *routine_;
  bool active_;

  one_shot_req info_;

  void inline __attribute__ ((always_inline))
      yield_to(yield_func_t &yield) {
    active_ = true;
    yield(*routine_);
  }

  // release myself from the routine list
  void inline __attribute__ ((always_inline))
      yield_from_routine_list(yield_func_t &yield) {
    assert(active_ == true);
    auto next  = next_;
    prev_->next_ = next;
    next->prev_  = prev_;
    if(routine_tailer == this)
      routine_tailer = prev_;
    active_ = false;
    next->yield_to(yield);
  }

  void inline  __attribute__ ((always_inline))
      add_to_routine_list() {
    if(active_)
      return; //skip add to the routine chain
    auto prev = routine_tailer;
    prev->next_ = this;
    routine_tailer = this;
    routine_tailer->next_ = routine_header;
    routine_tailer->prev_ = prev;
  }

} __attribute__ ((aligned(CACHE_LINE_SZ)));

inline __attribute__ ((always_inline))
RoutineMeta *get_routine_meta(int id) {
  return next_routine_array + id;
}

void inline __attribute__((always_inline)) register_one_shot(one_shot_func_t f,int id) {
  one_shot_callbacks[id] = f;
}

void inline __attribute__((always_inline)) add_to_routine_list(int id) {
  return next_routine_array[id].add_to_routine_list();
}

void inline __attribute__((always_inline)) add_one_shot_routine(int fm,int fc,int req_id,char *msg) {
  assert(!ro_pool->empty());
  RoutineMeta *meta = ro_pool->front();
  ro_pool->pop();

  // prepare the info
  meta->info_.from_mac = fm;
  meta->info_.from_routine = fc;
  meta->info_.req_id = req_id;
  meta->info_.msg = msg;

  meta->add_to_routine_list();
}

void inline print_chain() {
  // for debugging
}

} // namespace oltp
} // namespace nocc



#endif
