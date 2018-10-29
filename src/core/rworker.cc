#include "rocc_config.h"
#include "tx_config.h"

#include "util/util.h"
#include "util/mapped_log.h"

#include "rworker.h"
#include "routine.h"

// rdma related libs
#include "tcp_adapter.hpp"

#include "utils/amd64.h" // for nop pause

#include "logging.h" // for prints

// system or library headers
#include <queue>
#include <unistd.h>

#include <atomic>
#include <chrono> // for print current system time in a nicer way

#include <boost/bind.hpp>

using namespace std;
using namespace rdmaio;

namespace nocc {

// a dedicated routine is used to poll network events
#define MASTER_ROUTINE_ID 0

// per-thread log handler
__thread MappedLog local_log;

namespace oltp {

// per coroutine random generator
__thread util::fast_random *random_generator = NULL;

// A new event loop channel
void  RWorker::new_master_routine(yield_func_t &yield,int cor_id) {

  LOG(1) << "Worker " << worker_id_ << " on cpu " << sched_getcpu();

  auto routine_meta = get_routine_meta(MASTER_ROUTINE_ID);

  while( true ) {
    if(unlikely(!running_status())) {
      return exit_handler();
    }

    // poll events, will add ready routine back to the scheduler
    events_handler();

    // schedule the next routine
    auto next = routine_header->next_;
    if(next != routine_meta) {
      // set contexts
      change_ctx(next->id_);
      cor_id_ = next->id_;
      routine_meta_ = next;
      next->yield_to(yield);
    }
    // end main worker forever loop
  }
}

int RWorker::choose_rnic_port() {
  /**
   * Decide which NIC to use
   * Warning!! This is the setting on our platform.
   * It should be overwritten, according to the usage platform.
   * The detailed configuration can be queried using cm.
   */

  // default as 1.
  use_port_ = 1;

  int total_devices = cm_->query_devs().size();
  assert(total_devices > 0);

  if(total_devices <= 1)
    use_port_ = 0;
#if 0
  if(worker_id_ >= util::CorePerSocket()) {
    use_port_ = 0;
  }
#endif
  return use_port_;
}

void RWorker::init_rdma(char *rbuffer,uint64_t rbuf_size) {

  if(!USE_RDMA) // avoids calling cm on other networks
    return;

  choose_rnic_port();

  RdmaCtrl::DevIdx idx = cm_->convert_port_idx(use_port_);
  LOG(0) << "worker " << worker_id_ << " uses " << "["
         << idx.dev_id << "," << idx.port_id << "]";

  // open the device handler
  ASSERT(cm_->open_device(idx) != nullptr);

  // register the RDMA buffer
  ASSERT(cm_->register_memory(worker_id_,rbuffer,rbuf_size,cm_->get_device()) == true);
}

void RWorker::create_qps(int num) {

  if(!USE_RDMA) {
    return;
  }

  LOG(1) << "using RDMA device: " << use_port_ << " to create qps @" << worker_id_;
  assert(use_port_ >= 0); // check if init_rdma has been called

  ASSERT(false);
}

void RWorker::create_routines(int coroutines) {

  // init Ralloc, which will allocate memory on RDMA region
  RThreadLocalInit();

  // first per-routine data structure
  routines_         = new coroutine_func_t[1 + coroutines];
  random_generator  = new util::fast_random[1 + coroutines];

  for(uint i = 0;i < 1 + coroutines;++i){
    random_generator[i].set_seed0(rand_generator_.next());
  }

  routines_[0] = coroutine_func_t(bind(&RWorker::new_master_routine,this, _1, 0));

  // bind specific worker_routine
  for(uint i = 1;i <= coroutines;++i) {
    routines_[i] = coroutine_func_t(bind(&RWorker::worker_routine,this, _1));
  }

  // create RDMA scheduler
  rdma_sched_ = new RScheduler();
  rdma_sched_->thread_local_init(coroutines); // init rdma sched

  // init coroutine related data, 256: normal, 512: large scale
  RoutineMeta::thread_local_init(512,coroutines,routines_,this);

  total_worker_coroutine = coroutines;

  // done
  //inited = true;
}

void RWorker::create_client_connections(int total_connections) {

  if(server_type_ == UD_MSG && msg_handler_ != NULL) {
    client_handler_ = static_cast<UDAdapter *>(msg_handler_);
  } else {
    // create one
    ASSERT(cm_ != NULL) << "Currently client connection uses UD.";
    if(rpc_ == NULL)
      rpc_ = new RRpc(worker_id_,total_worker_coroutine);

    msg_handler_ = new UDAdapter(cm_,cm_->get_device(),cm_->get_local_mr(worker_id_),
                                 worker_id_,2048,
                                 std::bind(&RRpc::poll_comp_callback,rpc_,
                                           std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
    if(msg_handler_ == NULL) {
      msg_handler_ = client_handler_;
      rpc_->set_msg_handler(msg_handler_);
    }
  }
}

void RWorker::create_rdma_rc_connections(char *start_buffer,uint64_t total_ring_sz,uint64_t total_ring_padding) {
#if 0
  ASSERT(USE_RDMA);
  ASSERT(rpc_ == NULL && msg_handler_ == NULL);
  rpc_ = new RRpc(worker_id_,total_worker_coroutine);
  msg_handler_ = new RingMessage(total_ring_sz,total_ring_padding,worker_id_,cm_,start_buffer,
                                 std::bind(&RRpc::poll_comp_callback,rpc_,
                                           std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
  rpc_->set_msg_handler(msg_handler_);
#endif
  ASSERT(false); // not implemented
  server_type_ = RC_MSG;
}

// must be called after init rdma
void RWorker::create_rdma_ud_connections(int total_connections) {

  assert(USE_RDMA);
  rpc_ = new RRpc(worker_id_,total_worker_coroutine);

  msg_handler_ = new UDAdapter(cm_,cm_->get_device(),cm_->get_local_mr(worker_id_),
                               worker_id_,2048,
                               std::bind(&RRpc::poll_comp_callback,rpc_,
                                         std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
  // need connect
  rpc_->set_msg_handler(msg_handler_);

  server_type_ == UD_MSG;
}

void RWorker::create_tcp_connections(util::SingleQueue *queue, int tcp_port, zmq::context_t &context) {

  assert(msg_handler_ == NULL && rpc_ == NULL);  // not overwrite a previous connection
  rpc_ = new RRpc(worker_id_,total_worker_coroutine);

  auto adapter = new Adapter(std::bind(&RRpc::poll_comp_callback,rpc_,
                                       std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
                             cm_->current_node_id(),
                             worker_id_,queue);
#if DEDICATED == 1
  adapter->create_dedicated_sockets(cm_->network_,tcp_port,context);
#endif  // create sockets for the adapter
  msg_handler_ = adapter;
  rpc_->set_msg_handler(msg_handler_);

  server_type_ = TCP_MSG;
}

void RWorker::create_logger() {
  char log_path[16];
  sprintf(log_path,"./%d_%d.log",cm_->current_node_id(),worker_id_);
  new_mapped_log(log_path, &local_log,4096);
}

__thread RWorker *RWorker::thread_worker = NULL;

}// end namespace oltp
} // end namespace nocc
