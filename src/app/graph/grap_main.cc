#include "graph_constants.h"
#include "graph.h"
#include "graph_util.hpp"
#include "real_distribution.hpp"

#include "framework/bench_runner.h"

#include "util/util.h"

// for log normal distribution
#include <random>
#include <cmath>

using namespace nocc::util;

namespace nocc {
  namespace oltp {
  namespace link { // link benchmark

    class GraphLoader : public BenchLoader {
      MemDB *store_;
      uint64_t start_id_; uint64_t end_id_;
      double medium_data_size_;
    public:
      GraphLoader(uint64_t start_id,uint64_t end_id,double mdz,
                  uint64_t seed,int partition,MemDB *store,bool is_primary)
        : start_id_(start_id), end_id_(end_id), medium_data_size_(mdz),
          BenchLoader(seed)
      {
      };
      virtual void load();
    };

    class GraphRunner : public BenchRunner {
    public:
      GraphRunner(std::string &config_file) {

      }

      virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB* store = NULL);
      virtual std::vector<RWorker *> make_workers();
      virtual void init_store(MemDB* &store) { }
      virtual void init_backup_store(MemDB* &store) { }
      virtual void populate_cache() { }

      virtual void bootstrap_with_rdma(RdmaCtrl *r) {
      }

      virtual void warmup_buffer(char *buffer) {
      }
    };

    void GraphTest(int argc,char **argv) {
      auto test = RealDistribution();
      df_t res;

      std::ifstream ifs("../data/Distribution.dat", std::ifstream::in);
      auto title = test.get_cdf(res,ifs);
      pdf_t pdf;
      test.get_pdf(res,pdf);
      for(uint i = 0;i < 12;++i) {

      }
      //test.get_cdf(res,ifs);

      return;
    }

    std::vector<BenchLoader *> GraphRunner::make_loaders(int partition, MemDB* store) {
      std::vector<BenchLoader *> res;
      return res;
    }

    std::vector<RWorker *> GraphRunner::make_workers() {
      std::vector<RWorker *> res;
      return res;
    }

    void GraphLoader::load() {

    }


  }; // end namespace link
  }; // end namespace oltp
};   // end namespace nocc
