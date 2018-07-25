#include "tcp_adapter.hpp"

// a dummy cc file for static member's allocations
namespace nocc {

/**
 * Global poller for TCP based communication
 */
zmq::context_t recv_context(1);
zmq::context_t send_context(1);
AdapterPoller *poller = NULL;
std::vector<SingleQueue *>   local_comm_queues;

std::vector<zmq::socket_t *> Adapter::sockets;
std::vector<std::mutex *>    Adapter::locks;


}; // namespace nocc
