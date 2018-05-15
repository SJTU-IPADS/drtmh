#pragma once

#include <stdint.h>

#include "util/util.h"
using namespace nocc::util;

#include "base_distribution.hpp"

namespace nocc {

  namespace oltp {

    namespace link {

      class RealDistribution : public BaseDistribution {
        // constants
        static const uint64_t  NLINKS_SHUFFLER_SEED = 20343988438726021L;
        static const int       NLINKS_SHUFFLER_GROUPS = 1024;
        static const uint64_t  UNCORR_SHUFFLER_SEED = 53238253823453L;
        static const int       UNCORR_SHUFFLER_GROUPS = 1024;
        static const uint64_t  WRITE_CORR_SHUFFLER_SEED = NLINKS_SHUFFLER_SEED;
        static const int       WRITE_CORR_SHUFFLER_GROUPS = NLINKS_SHUFFLER_GROUPS;

        static const uint64_t  WRITE_UNCORR_SHUFFLER_SEED = UNCORR_SHUFFLER_SEED;
        static const int       WRITE_UNCORR_SHUFFLER_GROUPS = UNCORR_SHUFFLER_GROUPS;

        static const uint64_t  READ_UNCORR_SHUFFLER_SEED = UNCORR_SHUFFLER_SEED;
        static const int       READ_UNCORR_SHUFFLER_GROUPS = UNCORR_SHUFFLER_GROUPS;

        static const uint64_t  NODE_READ_SHUFFLER_SEED = 4766565305853767165L;
        static const int       NODE_READ_SHUFFLER_GROUPS = 1024;
        static const uint64_t  NODE_UPDATE_SHUFFLER_SEED = NODE_READ_SHUFFLER_SEED;
        static const int NODE_UPDATE_SHUFFLER_GROUPS = NODE_READ_SHUFFLER_GROUPS;

        static const uint64_t  NODE_DELETE_SHUFFLER_SEED = NODE_READ_SHUFFLER_SEED;
        static const int       NODE_DELETE_SHUFFLER_GROUPS = NODE_READ_SHUFFLER_GROUPS;

        enum TYPE { // types of distributions for different operation
          LINKS,    // distribution for links
          LINKS_READ,
          LINKS_WRITE,
          NODE_READ,
          NODE_WRITE
        };

        void init_link() {

        }

        uint64_t getNLinks(uint64_t id1,uint64_t start_id,uint64_t end_id) {
          return (uint64_t) expected_count(start_id,end_id,id1,nlinks_cdf_);
        }

      private:
        df_t nlinks_cdf_;
      }; // end class real

    };   // end class link
  };

};
