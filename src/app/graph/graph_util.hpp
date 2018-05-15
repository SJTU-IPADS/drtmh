#pragma once

// for std random generators
#include <random>
#include <cmath>
#include <math.h>  // for logs

namespace nocc {

  namespace oltp {

    namespace link {

      class LogNormalDistWrapper {

        std::mt19937 gen_;
        std::lognormal_distribution<> rng_;
        int min_;
        int max_;
        double mu_;
        double sigma_;

      public:
        LogNormalDistWrapper(int min,int max,int medium, double sigma,uint64_t seed)
          :gen_(seed),
           rng_(),
           min_(min),
           max_(max),
           sigma_(sigma),
           mu_(std::log(medium))
        {
        }

        int choose() {
          double next_guassian = rng_(gen_);
          assert(0 <= next_guassian && next_guassian <= 1);
          double content = next_guassian * sigma_ + mu_;
          int    choice = (int)(std::round(std::exp(content)));
          if(choice < min_) return min_;
          if(choice > max_) return max_ - 1;
          return choice;
        }
      }; // end class log normal wrapper
    };
  };

};
