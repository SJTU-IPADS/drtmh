#include "tcp_adapter.hpp"

// a dummy cc file for static member's allocations
namespace nocc {

  std::vector<zmq::socket_t *> Adapter::sockets;
  std::vector<std::mutex *>    Adapter::locks;

}; // namespace nocc
