/**
 * This file is included in the *class definiation file*.
 * It provides utilities to help select RC qp for the remote servers.
 * We use this file to help using multiple QPs to connection to the same server.
 *
 * Pre-assumptions:
 *   The class should contains a qp_vec_, which is type std::vector<rdmaio::Qp *> to store RC qps.
 */

#ifdef LARGE_CONNECTION
// FIXME: now only use 16 machines
int qp_idx_[16];

// Use a large number of connections
inline __attribute__((always_inline))
rdmaio::Qp* get_qp(int pid){
  int idx = (qp_idx_[pid]++) % QP_NUMS;
  return qp_vec_[pid*QP_NUMS + idx];
}

void fill_qp_vec(rdmaio::RdmaCtrl *cm,int wid) {

  // get rc qps
  auto num_nodes = cm->get_num_nodes();
  for(uint i = 0;i < num_nodes;++i) {
    for(uint j = 0;j < QP_NUMS ; j++){
      rdmaio::Qp *qp = cm->get_rc_qp(wid,i,j);
      assert(qp != NULL);
      qp_vec_.push_back(qp);
    }
  }
  // init the first QP idx to 0
  memset(qp_idx_,0,sizeof(int)*16);
}
#else

inline __attribute__((always_inline))
rdmaio::Qp* get_qp(int pid) {
  return qp_vec_[pid];
}

#endif
