/**
 * This file is included in the *class definiation file*.
 * It provides utilities to help select RC qp for the remote servers.
 * We use this file to help using multiple QPs to connection to the same server.
 *
 * Pre-assumptions:
 *   The class should contains a qp_vec_, which is type std::vector<rdmaio::Qp *> to store RC qps.
 */
// QP vector to store all QPs
std::vector<Qp *> qp_vec_;
uint qp_idx_[16];

#if LARGE_CONNECTION
// FIXME: now only use 16 machines

// Use a large number of connections
inline __attribute__((always_inline))
rdmaio::Qp* get_qp(int pid){
  int idx = (qp_idx_[pid]++) % QP_NUMS;
  Qp *qp = qp_vec_[pid * QP_NUMS + idx];
  ASSERT(qp->nid == pid);
  return qp;
}

#else

inline __attribute__((always_inline))
rdmaio::Qp* get_qp(int pid) {
  return qp_vec_[pid];
}

#endif


void fill_qp_vec(rdmaio::RdmaCtrl *cm,int wid) {

  assert(qp_vec_.size() == 0);
  // get rc qps
  auto num_nodes = cm->get_num_nodes();
  for(uint i = 0;i < num_nodes;++i) {
    for(uint j = 0;j < QP_NUMS; j++){
      rdmaio::Qp *qp = cm->get_rc_qp(wid,i,j);
      assert(qp != NULL);
      qp_vec_.push_back(qp);
    }
  }
#if LARGE_CONNECTION
  // init the first QP idx to 0
  memset(qp_idx_,0,sizeof(qp_idx_));
#endif
}

// snaity checks
static_assert(QP_NUMS >= 1,"Each mac requires at least one QP!");
#if !LARGE_CONNECTION
static_assert(QP_NUMS == 1,"If not use a large connection, we should use exactly one QP to link to another node.");
#endif
