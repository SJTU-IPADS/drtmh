#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include "framework/bench_runner.h"
#include "micro_worker.h"

extern std::vector<std::string> cluster_topology;
extern uint64_t ops_per_worker;

namespace nocc {

namespace oltp {

namespace micro {

uint64_t working_space = 8 * 1024 * 1024;
unsigned g_txn_workload_mix[1] = { 100 };

int micro_type = 0;

class MicroRunner : public BenchRunner {
 public:
  MicroRunner(std::string &config_file) :
      BenchRunner(config_file) {

    // parse the TX mix ratio
    using boost::property_tree::ptree;
    using namespace boost;
    using namespace property_tree;

    ptree pt;
    read_xml(config_file,pt);

    // parse input xml
    try {
      int type = pt.get<int>("bench.micro");
      micro_type = type;

    } catch(const ptree_error &e) {
      assert(false);
    }

    try {
      working_space = pt.get<uint64_t>("bench.space");
      working_space = working_space * (1024 * 1024);
      fprintf(stdout,"working space %f\n",get_memory_size_g(working_space));
    } catch(const ptree_error &e) {

    }
  } // constructer of MicroMainRunner

  std::vector<RWorker *> make_workers() {

    std::vector<RWorker *> ret;
    fast_random r(23984543 + current_partition);

	for(uint i = 0;i < nthreads; ++i) {
      ret.push_back(new MicroWorker(i,r.next(),micro_type,store_,ops_per_worker,
                                    &barrier_a_,&barrier_b_,
                                    static_cast<BenchRunner *>(this)));
	}
    return ret;
  }
};

// main hook function
void MicroTest(int argc,char **argv) {
  LOG(3) << "micro started";
  MicroRunner runner(nocc::oltp::config_file_name);
  runner.run();
}


MicroWorker::MicroWorker(unsigned int worker_id,unsigned long seed,int micro_type,MemDB *store,
						 uint64_t total_ops, spin_barrier *a,spin_barrier *b,BenchRunner *r):
    BenchWorker(worker_id,true,seed,total_ops,a,b,r) {

}


void MicroWorker::thread_local_init() {

  for(uint i = 0;i < cluster_topology.size();++i) {
    RCQP *qp = cm_->get_rc_qp(create_rc_idx(i,worker_id_));
    ASSERT(qp != nullptr) << "get null QP at " << i << " " << worker_id_;
    qp_vec_.push_back(qp);
  }
}

workload_desc_vec_t MicroWorker::get_workload() const {
  return _get_workload();
}

workload_desc_vec_t MicroWorker::_get_workload() {

  workload_desc_vec_t w;

  std::string name_m;
  txn_fn_t fn;

  switch (micro_type) {
    case RPC: {
      name_m = "Rpc request tests"; fn = MicroRpc;
      break;
    }
    case RDMA_READ: {
      name_m = "RDMA READ"; fn = MicroRdmaRead;
    }
      break;
    case RDMA_WRITE:
      {
        name_m = "RDMA WRITE"; fn = MicroRdmaWrite;
      }
      break;
    default: {
      LOG(LOG_WARNING) << "unknown micro type: " << micro_type;
    }
    case RDMA_CAS:
      {
        name_m = "RDMA ATOMICs"; fn = MicroRdmaAtomic;
      }
      break;
  }
  w.push_back(workload_desc(name_m,double(g_txn_workload_mix[0]) / 100.0,fn));
  return w;
}

} // namespace micro
}
}
