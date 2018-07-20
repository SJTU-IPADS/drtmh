#ifndef RTX_BENCH_LISTENER
#define RTX_BENCH_LISTENER

#include "bench_worker.h"
#include "bench_reporter.h"

namespace nocc {

namespace oltp {

class BenchLocalListener : public RWorker {
 public:
  BenchLocalListener(unsigned worker_id,const std::vector<RWorker *> &workers,
                     BenchReporter *reporter);

  /**
   *  Main thread body of bench_listener.
   */
  virtual void run();

  /**
   * Main worker routine.
   */
  virtual void worker_routine(yield_func_t &yield);

 private:
  /**
   * End the benchmark, if necessary.
   */
  void ending();

  /**
   * Worker routine at the master.
   * It start slaves, collect results from others
   */
  void worker_routine_master(yield_func_t &yield);


  /**
   * Worker routine at the slave.
   * It send results (throughput,etc) to the master
   */
  void worker_routine_slave(yield_func_t &yield);

  /**
   * Used to monitor user inputs.
   */
  void sigint_handler(int);

  /**
   * handler slave's initilization done RPCs.
   */
  void init_rpc_handler(int id,int cid,char *msg,void *arg);

  /**
   * handler the start requests
   */
  void start_rpc_handler(int id,int cid,char *msg,void *arg);

  /**
   * handler slave's requests
   */
  void get_result_rpc_handler(int id,int cid,char *msg,void *arg);

  /**
   * handler the exit request
   */
  void exit_rpc_handler(int id,int cid,char *msg,void *arg);



 private:
  int epoch_ = 0;                      // execution epoch
  int nresult_returned_ = 0;           // used to collect return results
  BenchReporter *reporter_ = NULL;     // the reporter used to parse results
  std::vector<BenchWorker *> workers_; // record current worker at this machine
};

} // end namespace oltp
} // end namespace nocc

#endif
