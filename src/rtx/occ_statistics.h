/**
 * This file should be included in occ.h
 */

LAT_VARS(lock);
LAT_VARS(validate);
LAT_VARS(commit);
LAT_VARS(log);
LAT_VARS(temp);

#if NOCC_STATICS
CountVector<double> lock_;
CountVector<double> validate_;
CountVector<double> commit_;
CountVector<double> log_;
CountVector<double> temp_;

void report_statics(uint64_t one_second) {

  lock_.erase(0.1);
  //validate_.erase(0.1);
  commit_.erase(0.1);
  log_.erase(0.1);
  temp_.erase(0.1);

  LOG(4) << "lock time: "    << util::BreakdownTimer::rdtsc_to_ms(lock_.average(),one_second) << "ms";
  LOG(4) << "logging time: " << util::BreakdownTimer::rdtsc_to_ms(log_.average(),one_second) << "ms";
  LOG(4) << "commit time: "  << util::BreakdownTimer::rdtsc_to_ms(commit_.average(),one_second) << "ms";
  LOG(4) << "temp time: "  << temp_.average() << "cycle";
}

void record() {
  double res;
  REPORT_V(lock,res);
  lock_.push_back(res);

  REPORT_V(log,res);
  log_.push_back(res);

  REPORT_V(commit,res);
  commit_.push_back(res);

  REPORT_V(temp,res);
  temp_.push_back(res);
}

#else
// no counting, return null
void report_statics(uint64_t) {

}

void record() {

}
#endif
