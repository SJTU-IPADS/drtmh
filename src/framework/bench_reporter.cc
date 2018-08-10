#include "config.h"
#include "bench_reporter.h"

#include <string>
#include <string.h>
#include <vector>

extern size_t nthreads;
#define MIN(x,y) ((x) > (y)?(y):(x))

// an example reporter for the framework

// the data exchanged between servers
struct WorkerData {
  double throughput;
  int32_t aborts;
  int32_t abort_ratio;
};

static std::string normalize_throughput(uint64_t thpt) {

  static uint64_t K = 1000;
  static uint64_t M = K * 1000;

  char buf[64];
  if(thpt > M) {
    sprintf(buf,"%f M",thpt / (double)M);
  } else if(thpt > K) {
    sprintf(buf,"%f K",thpt / (double)K);
  } else {
    sprintf(buf,"%lu",thpt);
  }
  return std::string(buf);
}

namespace nocc {

namespace oltp {

uint64_t second_cycle;

std::vector<double> thpts;

size_t BenchReporter::data_len() { return sizeof(WorkerData);}

void BenchReporter::init(const std::vector<BenchWorker *> *workers) {

  workers_ = workers;

  prev_commits_.clear();
  prev_aborts_.clear();
  prev_abort_ratio_.clear();

  auto num_of_worker = workers_->size();

  for(uint i = 0;i < num_of_worker;++i) {
    prev_commits_.push_back(0);
    prev_aborts_.push_back(0);
    prev_abort_ratio_.push_back(0);
  }

  throughput = 0;aborts = 0;abort_ratio = 0;
  second_cycle = util::Breakdown_Timer::get_one_second_cycle();

  for(uint i = 0;i < total_partition;++i) {
    throughputs.push_back(0.0);
  }
}


void BenchReporter::merge_data(char *data,int id) {
  WorkerData *p = (WorkerData *)data;
  assert(p->throughput < 1000000000L);
  //throughput += p->throughput;
  //aborts = p->aborts;
  //abort_ratio += p->abort_ratio;
  fprintf(stdout,"merge data %s from %s\n", normalize_throughput(p->throughput).c_str(),
          cm->network_[id].c_str());
  throughputs[id] = p->throughput;
}

void BenchReporter::collect_data(char *data,struct  timespec &start_t) {
  (*workers_)[0]->workload_report();
  // calculate results
  uint64_t res = calculate_commits(prev_commits_);
  uint64_t abort_num = calculate_aborts(prev_aborts_);
  double   abort_ratio = calculate_abort_ratio(prev_abort_ratio_);

  // re-set timer
  struct timespec end_t;
  clock_gettime(CLOCK_REALTIME, &end_t);
  double elapsed_sec = util::DiffTimespec(end_t,start_t) / 1000.0;
  clock_gettime(CLOCK_REALTIME, &start_t);

  double my_thr = (double)res / elapsed_sec;

  fprintf(stdout,"my throughput %s, ratio %f\n",normalize_throughput(my_thr).c_str(),abort_ratio);

  WorkerData *p = (WorkerData *)data;
  p->throughput = my_thr;
  p->aborts = abort_num;
  p->abort_ratio = abort_ratio;
  return;
}

void BenchReporter::report_data(uint64_t epoch,std::ofstream &log_file) {

  (*workers_)[0]->workload_report();
  // calculate latency
  auto latency = 0;
  //latency = (*workers_)[0]->latency_timer_.report() / second_cycle * 1000;

  auto sum = 0.0;
  for(auto i = 0;i < throughputs.size();++i) {
    sum += throughputs[i];
  }

  double abort_ratio = calculate_abort_ratio(prev_abort_ratio_);
#if LISTENER_PRINT_PERF == 1
  fprintf(stdout,"@%lu System throughput %s, abort %f\n",
          epoch,normalize_throughput(sum).c_str(),
          abort_ratio);
  fprintf(stdout,"succ ratio %f\n", calculate_execute_ratio());
#endif

#ifdef LOG_RESULTS
  if(epoch > 10 && log_file.is_open()) {
    /* warm up for 5 seconds, also the calcuation script will skip some seconds*/
    /* record the result */
    log_file << (sum) << " "<< abort_ratio <<" "
             << latency << std::endl;
    thpts.push_back(throughput);
  }
#endif

  // clear the report data
  throughput = 0;
  aborts = 0;
  abort_ratio = 0;
}

double BenchReporter::calculate_execute_ratio() {
#if 0
  uint64_t n_exes = (*workers_)[0]->ntxn_executed_;
  uint64_t n_commits = (*workers_)[0]->ntxn_commits_;

  double s = (double)(n_exes - prev_executed);
  double v = (double)(n_commits - prev_commits);
  prev_executed = n_exes;
  prev_commits  = n_commits;
  return v / s;
#endif
}


double BenchReporter::calculate_abort_ratio(std::vector<uint64_t> &prevs) {
  double res = (double)((*workers_)[0]->ntxn_abort_ratio_) / (double)((*workers_)[0]->ntxn_executed_);
  return res;
}


uint64_t BenchReporter::calculate_commits(std::vector<uint64_t> &prevs) {

  uint64_t sum = 0;
  for(uint i = 0;i < nthreads;++i) {

    uint64_t snap = (*workers_)[i]->ntxn_commits_;
    auto res = snap - prevs[i];
    //if(res == 0)
    //fprintf(stdout,"worker %d, %lu\n",i,res);
    sum += res;
    assert(snap >= prevs[i]);
    prevs[i] = snap;
  }

  return sum;
}

uint64_t BenchReporter::calculate_aborts(std::vector<uint64_t> &prevs) {

  uint64_t sum = 0;
  for(uint i = 0;i < nthreads;++i) {
    uint64_t snap = (*workers_)[i]->ntxn_aborts_;
    sum += (snap - prevs[i]);
    assert(snap >= prevs[i]);
    prevs[i] = snap;
  }
  aborts += sum;
  return sum;
}

void BenchReporter::end() {
  if(thpts.size() == 0) return;
  double sum = 0;int c(0);
  for(uint i = 2;i < MIN(thpts.size(),i + 10);++i) {
    sum += thpts[i];c++;
  }
  fprintf(stdout,"avg thpt: %f\n",sum / c);
}

}; // namespace oltp


}; // namespace nocc
