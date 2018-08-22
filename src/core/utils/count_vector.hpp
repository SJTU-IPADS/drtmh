#pragma once

#include <vector>

namespace nocc {

// a vector class to store count data
template <typename T>
class CountVector : public std::vector<T> {
 public:
  CountVector() :
      std::vector<T>()
  {
  }

  double average() {

    if(std::vector<T>::size() == 0)
      return 0.0;

    T sum = 0;
    for(auto it = std::vector<T>::begin();it != std::vector<T>::end();++it) {
      sum += *it;
    }
    double res = static_cast<double>(sum) / std::vector<T>::size();
    return res;
  }

  // erase the first ratio% and last ratio% items
  void erase(double ratio) {

    // If there are too less elements in the vector, not erase items
    if(std::vector<T>::size() < 5)
      return;

    int temp_size = std::vector<T>::size();
    int idx = std::floor(std::vector<T>::size() * ratio);
    std::vector<T>::erase(std::vector<T>::begin() + temp_size - idx,std::vector<T>::end());
    std::vector<T>::erase(std::vector<T>::begin(),std::vector<T>::begin() + idx);
  }

  void sort() {
    std::sort(std::vector<T>::begin(),std::vector<T>::end());
  }
};

} // namespace nocc
