#ifndef NOCC_BENCH_RUNNER
#define NOCC_BENCH_RUNNER

#include "bench_worker.hpp"
#include "backup_worker.h"

namespace nocc {

  namespace oltp {

    // global structure provided
    extern View* my_view; // replication setting of the data
    extern std::string config_file_name;

    std::vector<std::string> parse_network(int num, std::string &hosts);

    /* Bench runner is used to bootstrap the application */
    class BenchRunner {
    public:
      BenchRunner(std::string &config_file);
    BenchRunner() : barrier_a_(1), barrier_b_(1) {}
      std::vector<std::string> net_def_;

      void run();
    protected:
      /* parse the arguments the application requires, if necessary*/
      void    parse_config(std::string &config_file);

      /* Inputs:
         partition: partition id of the database
         store:     the local store handler
       */
      virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB *store = NULL) = 0;

      /* return a set of workers to execute application logic */
      virtual std::vector<Worker *> make_workers() = 0;

      /* make some background workers, if necessary */
      virtual std::vector<BackupBenchWorker *> make_backup_workers() = 0;


      /*   below 2 functions are used to init data structure
           The first is called before any RDMA connections are made, which is used ti init
           data structures resides on RDMA registered area.
           The second is called after RDMA connections are made, which is used to init global
           data structure that needs RDMA for communication
      */
      /* warm up the rdma buffer, can be replaced by the application */
      virtual void warmup_buffer(char *buffer) = 0;
      virtual void bootstrap_with_rdma(RdmaCtrl *r) = 0;

      // cache the remote addresses of the key-value store
      virtual void populate_cache() {}

      virtual void init_store(MemDB* &store) = 0;
      virtual void init_backup_store(MemDB* &store) = 0;
      virtual void init_put() = 0;

      spin_barrier barrier_a_;
      spin_barrier barrier_b_;
      MemDB *store_;
      MemDB *backup_stores_[MAX_BACKUP_NUM];
    private:
      BenchListener *listener_;
      SpinLock rdma_init_lock_;
      int8_t   init_worker_count_; /*used for worker to notify qp creation done*/
    };

  } // namespace oltp
};  // namespace nocc

#endif
