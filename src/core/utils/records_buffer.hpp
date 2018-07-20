#pragma once

/*
 * RecordsBuffer helpes count the frequencies of events
 */

#include <vector>
#include <fstream>
#include <iterator>

namespace nocc {
namespace util {

template <typename T>
class RecordsBuffer {
  const uint64_t max_elems = 10000000;
  static const std::vector<int> default_cdf;
 public:
  RecordsBuffer() {

  }

  inline bool add(T t) {
    if(buffer.size() + 1 > max_elems)
      return false;
    buffer.push_back(t);
    return true;
  }

  // merge another buffer's results
  inline void add(RecordsBuffer<T> &r) {
    if(r.buffer.size() == 0)
      return;
    buffer.insert(buffer.end(),r.buffer.begin(),r.buffer.end());
  }

  uint64_t size() {
    return buffer.size();
  }

  T & operator[] (int idx) {
    return buffer[idx];
  }

  // sort the buffer, including elimiate some elemets
  inline void sort_buffer(double percent = 0.1) {
    if(buffer.size() == 0) return;

    // now erase
    int temp_size = buffer.size();;
    int idx = std::floor(buffer.size() * percent / 100.0);
    buffer.erase(buffer.begin(),buffer.begin() + idx + 1);
    buffer.erase(buffer.begin() + temp_size - idx, buffer.end());

    std::sort(buffer.begin(),buffer.end());
  }

  void dump_buffer(const char *file_name) const {
    std::ofstream output_file(file_name);
    for (const auto &e : buffer) output_file << e << "\n";
    output_file.close();
  }

  void dump_as_cdf(const char *file_name,const std::vector<int> &cdf = default_cdf) const {

    fprintf(stdout,"[Records buffer] Dump %u items to cdf.\n",buffer.size());
    if(buffer.size() == 0)
      return;
    std::ofstream output_file(file_name);
    for(uint i = 0;i < cdf.size();++i) {
      int idx = std::floor((double)buffer.size() * cdf[i] / 100);
      if(cdf[i] == 100) idx = buffer.size() - 1;
      output_file << cdf[i] << " " << buffer[idx] << std::endl;
    }
    output_file.close();
  }

  T report_medium() const {
    if(buffer.size() == 0) return 0;
    return buffer[buffer.size() / 2];
  }

  T report_90() const {
    if(buffer.size() == 0) return 0;
    int idx = std::floor( buffer.size() * 90 / 100.0);
    return buffer[idx];
  }

  T report_99() const {
    if(buffer.size() == 0) return 0;
    int idx = std::floor(buffer.size() * 99 / 100.0);
    return buffer[idx];
  }

  T report_avg() const {
    if(buffer.size() == 0) return 0;
    double average = 0;
    uint64_t count  = 0;
    for(uint i = 0;i < buffer.size();++i) {
      average += (buffer[i] - average) / (++count);
    }
    return average;
  }

 private:
  std::vector<T> buffer;

};// end class

template <typename T>
const std::vector<int> RecordsBuffer<T>::default_cdf = std::vector<int>(
    {1,5,10,15,20,25,30,35,40,45,
     50,55,60,65,70,75,80,85,90,95,
     97,98,99,100});


} // namespace util

}
