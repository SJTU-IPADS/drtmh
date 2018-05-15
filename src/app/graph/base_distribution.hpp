#pragma once

#include <map>
#include <vector>

#include "util/util.h"

using namespace nocc::util;

namespace nocc {
  namespace oltp {
    namespace link {

      typedef std::map<int,double> df_t;
      typedef std::vector<double> pdf_t;

      class BaseDistribution {

      public:
        std::string get_cdf(df_t &cdf,std::istream &input) {
          input.clear();

          std::string title;
          if(!NextString(input,title)) {
            assert(false);
          }
          int value;
          while(NextInt(input,value)) {
            double percent;
            assert(NextDouble(input,percent));
            double probability = percent / 100;
            assert(cdf.find(value) == cdf.end());
            cdf.insert(std::make_pair(value,probability));
          }
          return title;
        }

        void get_pdf(df_t &cdf, pdf_t &pdf) {

          pdf.clear();
          if(cdf.size() == 0) return;
          pdf.reserve(cdf.size());

          auto it = cdf.begin();
          pdf[it->first] = it->second;
          for(uint i = 0;i < cdf.size() - 1;++i) {
            auto prev = it;
            auto cur  = it++;
            assert(cur->first > prev->first);
            pdf[cur->first] = cur->second - prev->second;
          }
        }

        void get_ccdf(pdf_t &pdf,pdf_t &ccdf) {
          int length = pdf.size();
          ccdf.clear();
          ccdf.reserve(length);

          ccdf[length - 1] = pdf[length - 1];
          for (int i = length - 2; i >= 0; --i) {
            ccdf[i] = ccdf[i + 1] + pdf[i];
          }
        }

        void get_cumulative_sum(pdf_t &cdf,pdf_t &cs) {

          int length = cdf.size();
          cs.clear();
          cs.reserve(length);

          cs[0] = 0; //ignore cdf[0]
          for (int i = 1; i < length; ++i) {
            cs[i] = cs[i - 1] + cdf[i];
          }
        }

        double expected_value(df_t &cdf) {
          if(cdf.size() == 0) return 0;

          auto it = cdf.begin();
          double sum = 0;
          sum = it->second * it->first;
          for(uint i = 0;i < cdf.size() - 1;++i) {
            auto prev = it;
            auto cur  = it++;
            assert(cur->first > prev->first);
            double p = cur->second - prev->second;
            sum += p * cur->first;
          }
          return sum;
        }

        typedef std::pair<int,double> pair_t;
        pair_t binary_search(df_t &df,double p) {

          std::vector<pair_t> points;
          assert(df.size() > 0);
          for(auto it = df.begin();it != df.end();++it) points.emplace_back(it->first,it->second);

          int left = 0, right = points.size() - 1;
          while (left < right) {
            int mid = (left + right)/2;
            if (points[mid].second >= p) {
              right = mid;
            } else {
              left = mid + 1;
            }
          }
          if (points[left].second >= p) {
            return points[left];
          } else {
            assert(left + 1 < points.size());
            return points[left + 1];
          }
          return points[0];
        }

        //double expected_count(uint64_t id) { return expected_count(min,max,id,cdf);}
        double expected_count(uint64_t min,uint64_t max,uint64_t id,df_t &cdf) {
          if (id < min || id >= max) {
            return 0.0;
          }
          uint64_t n = (max - min);
          // Put in into range [0.0, 1.0] with most popular at 0.0
          double u = 1.0 - (id - min) / (double) n;
          auto res = binary_search(cdf, u);
          assert(u <= res.second);
          // Assuming piecewise linear, so equally as probably as p1.value
          return res.first;
        }

      }; // end class



    }; // end namespace link
  };

};
