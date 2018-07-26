#include "rocc_config.h"
#include "tx_config.h"

#include "util/util.h"
#include "util/mapped_log.h"

#include "rworker.h"
#include "routine.h"

// rdma related libs
#include "ring_imm_msg.h"
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
using namespace rdmaio::ring_imm_msg;

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

  LOG(2) << "Worker " << worker_id_ << " on cpu " << sched_getcpu();

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

  int total_devices = cm_->query_devinfo();
  assert(total_devices > 0);

  if(total_devices <= 1)
    use_port_ = 0;

  if(worker_id_ >= util::CorePerSocket()) {
    use_port_ = 0;
  }
  return use_port_;
}

void RWorker::init_rdma() {

  if(!USE_RDMA) // avoids calling cm on other networks
    return;

  cm_->thread_local_init();
  choose_rnic_port();

  // get the device id and port id used on the nic.
  int dev_id = cm_->get_active_dev(use_port_);
  int port_idx = cm_->get_active_port(use_port_);
  ASSERT(port_idx > 0) << "worker " << worker_id_
                       << " get port idx " << port_idx;

  // open the specific RNIC handler, and register its memory
  cm_->open_device(dev_id);
  cm_->register_connect_mr(dev_id); // register memory on the specific device
}

void RWorker::create_qps() {

  if(!USE_RDMA) {
    return;
  }

  LOG(1) << "using RDMA device: " << use_port_ << " to create qps @" << worker_id_;
  assert(use_port_ >= 0); // check if init_rdma has been called

  int dev_id = cm_->get_active_dev(use_port_);
  int port_idx = cm_->get_active_port(use_port_);

  for(uint i = 0; i < QP_NUMS; i++){
    cm_->link_connect_qps(worker_id_, dev_id, port_idx, i, IBV_QPT_RC);
  }
  // note, link_connect_qps correctly handles duplicates creations
#if USE_UD_MSG == 0 && USE_TCP_MSG == 0 // use RC QP, thus create its QP
  cm_->link_connect_qps(worker_id_, dev_id, port_idx, 0, IBV_QPT_RC);
#endif // USE_UD_MSG
}

void RWorker::init_routines(int coroutines) {

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
  rdma_sched_ = new RDMA_sched();
  rdma_sched_->thread_local_init(coroutines); // init rdma sched

  // init coroutine related data, 256: normal, 512: large scale
  RoutineMeta::thread_local_init(512,coroutines,routines_,this);

  total_worker_coroutine = coroutines;

  // done
  //inited = true;
}

void RWorker::create_client_connections(int total_connections) {

  if(server_type_ == UD_MSG && msg_handler_ != NULL) {
    client_handler_ = static_cast<UDMsg *>(msg_handler_);
  } else {
    // create one
    ASSERT(cm_ != NULL) << "Currently client connection uses UD.";
    if(rpc_ == NULL)
      rpc_ = new RRpc(worker_id_,total_worker_coroutine);
    assert(use_port_ >= 0);
    int dev_id = cm_->get_active_dev(use_port_);
    int port_idx = cm_->get_active_port(use_port_);

    client_handler_ = new UDMsg(cm_,worker_id_,total_connections,
                                2048, // max concurrent msg received
                                std::bind(&RRpc::poll_comp_callback,rpc_,
                                          std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
                                dev_id,port_idx,1);
    if(msg_handler_ == NULL) {
      msg_handler_ = client_handler_;
      rpc_->set_msg_handler(msg_handler_);
    }
  }
}

void RWorker::create_rdma_rc_connections(char *start_buffer,uint64_t total_ring_sz,uint64_t total_ring_padding) {
  ASSERT(USE_RDMA);
  ASSERT(rpc_ == NULL && msg_handler_ == NULL);
  rpc_ = new RRpc(worker_id_,total_worker_coroutine);
  msg_handler_ = new RingMessage(total_ring_sz,total_ring_padding,worker_id_,cm_,start_buffer,
                                 std::bind(&RRpc::poll_comp_callback,rpc_,
                                           std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
  rpc_->set_msg_handler(msg_handler_);
}

// must be called after init rdma
void RWorker::create_rdma_ud_connections(int total_connections) {

  assert(cm_ != NULL);

  int dev_id = cm_->get_active_dev(use_port_);
  int port_idx = cm_->get_active_port(use_port_);

  assert(USE_RDMA);

  rpc_ = new RRpc(worker_id_,total_worker_coroutine);

  using namespace rdmaio::udmsg;
  msg_handler_ = new UDMsg(cm_,worker_id_,total_connections,
                           2048, // max concurrent msg received
                           std::bind(&RRpc::poll_comp_callback,rpc_,
                                     std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
                           dev_id,port_idx,1);
  rpc_->set_msg_handler(msg_handler_);
}

void RWorker::create_tcp_connections(util::SingleQueue *queue, int tcp_port, zmq::context_t &context) {

  assert(msg_handler_ == NULL && rpc_ == NULL);  // not overwrite a previous connection
  rpc_ = new RRpc(worker_id_,total_worker_coroutine);

  auto adapter = new Adapter(std::bind(&RRpc::poll_comp_callback,rpc_,
                                       std::placeholders::_1,std::placeholders::_2,std::placeholders::_3),
                             cm_->get_nodeid(),
                             worker_id_,queue);
#if DEDICATED == 1
  adapter->create_dedicated_sockets(cm_->network_,tcp_port,context);
#endif  // create sockets for the adapter
  msg_handler_ = adapter;
  rpc_->set_msg_handler(msg_handler_);
}

void RWorker::create_logger() {
  char log_path[16];
  sprintf(log_path,"./%d_%d.log",cm_->get_nodeid(),worker_id_);
  new_mapped_log(log_path, &local_log,4096);
}

__thread RWorker *RWorker::thread_worker = NULL;

}// end namespace oltp
} // end namespace nocc
