#include <mutex>
#include <boost/bind.hpp>

#include "routine.h"
#include "rworker.h"

namespace nocc {

// default routines to bind

namespace oltp {

__thread std::queue<RoutineMeta *> *ro_pool;

// add a routine chain to RPC
__thread RoutineMeta *next_routine_array;
__thread RoutineMeta *routine_tailer;   // the header of the scheduler
__thread RoutineMeta *routine_header;   // the tailer of the scheduler

__thread RoutineMeta *one_shot_routine_pool;

__thread one_shot_func_t *one_shot_callbacks;
__thread RWorker *worker = NULL;

bool inited = false;
std::mutex routine_mtx;

void one_shot_func(yield_func_t &yield,RoutineMeta *routine) {

  while(1) {
    // callback
    one_shot_callbacks[routine->info_.req_id](yield,routine->info_.from_mac,
                                              routine->info_.from_routine,routine->info_.msg,
                                              NULL);
    ro_pool->push(routine);
    free(routine->info_.msg);
    // yield back
    // need to reset-the context if necessary
    worker->indirect_must_yield(yield);
  }
}

void RoutineMeta::thread_local_init(int num_ros,int coroutines,coroutine_func_t *routines_,RWorker *w) {

  // init worker
  assert(worker == NULL);
  worker = w;

  // init next routine array
  next_routine_array = new RoutineMeta[coroutines + 1];

  for(uint i = 0;i < coroutines + 1;++i) {
    next_routine_array[i].id_   = i;
    next_routine_array[i].routine_ = routines_ + i;
  }

  for(uint i = 0;i < coroutines + 1;++i) {
    next_routine_array[i].prev_ = next_routine_array + i - 1;
    next_routine_array[i].next_ = next_routine_array + i + 1;
  }

  routine_header = &(next_routine_array[0]);
  routine_tailer = &(next_routine_array[coroutines]);

  // set master routine's status
  next_routine_array[0].prev_ = routine_header;

  // set tail routine's chain
  next_routine_array[coroutines].next_ = routine_header; //loop back

  // init ro routine pool
  ro_pool = new std::queue<RoutineMeta *>();
  for(uint i = 0;i < num_ros;++i) {
    RoutineMeta *meta = new RoutineMeta;
    meta->prev_ = NULL; meta->next_ = NULL; meta->id_ = 0;
    meta->routine_ = new coroutine_func_t(bind(one_shot_func,_1,meta));
    ro_pool->push(meta);
  }

  one_shot_callbacks = new one_shot_func_t[MAX_ONE_SHOT_CALLBACK];
}

void RoutineMeta::register_callback(one_shot_func_t callback,int id) {
  register_one_shot(callback,id);
}


};

};
