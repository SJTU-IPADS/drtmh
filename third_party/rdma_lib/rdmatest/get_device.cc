// This file prints the attribute of the NIC

#include "rdmaio.h"

#include <stdio.h>


using namespace rdmaio;

int main() {

  std::vector<std::string>  network;
  RdmaCtrl *cm = new RdmaCtrl(0,network,8888,false);

  cm->query_devinfo();
  cm->thread_local_init();
  cm->open_device(0);

  ibv_device_attr attr;
  cm->query_specific_dev(0,&attr);

  fprintf(stdout,"rd_max: %d, rd_max_dest %d\n",attr.max_qp_rd_atom,attr.max_res_rd_atom);

  return 0;

}
