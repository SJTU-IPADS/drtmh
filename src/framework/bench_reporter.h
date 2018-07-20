#ifndef RTX_BENCH_REPORTER
#define RTX_BENCH_REPORTER

#include "bench_worker.h"

namespace nocc {

namespace oltp {

class BenchReporter { // not thread safe
 public:
  virtual void init(const std::vector<BenchWorker *> *workers);
  virtual void merge_data(char *);
  virtual void report_data(uint64_t epoch,std::ofstream &log_file);
  virtual void collect_data(char *data,struct timespec &start_t); // the data is stored in *data
  virtual size_t data_len();
  virtual void end();

 private:
  double throughput;
  double aborts;
  double abort_ratio;

  std::vector<uint64_t> prev_commits_;
  std::vector<uint64_t> prev_aborts_;
  std::vector<uint64_t> prev_abort_ratio_;

  const std::vector<BenchWorker *> *workers_;

  // helper functions
  uint64_t calculate_commits(std::vector<uint64_t> &prevs);
  uint64_t calculate_aborts(std::vector<uint64_t> &prevs);
  double   calculate_abort_ratio(std::vector<uint64_t> &prevs);
  double   calculate_execute_ratio();
};

} // end namespace oltp

} // end namespace nocc

#endif
