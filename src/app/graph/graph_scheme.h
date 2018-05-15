#ifndef NOCC_APP_GRAPH_SCHEME_
#define NOCC_APP_GRAPH_SCHEME_

#include "all.h"

namespace nocc {
  namespace oltp {
    namespace link {

      struct Node {
        uint64_t id;
        int type;
        uint64_t version;
        uint64_t time;
        char data[CACHE_LINE_SZ];
      };

      class Edge {
        int  type;
        bool visiblilty;
        uint64_t version;
        char data[CACHE_LINE_SZ];
      };

      inline ALWAYS_INLINE
        uint64_t makeEdgeKey(uint64_t id0,uint64_t id1,uint64_t time) {
        uint64_t *sec = new uint64_t[3];
        sec[0] = id0; sec[1] = id1;sec[2] = time;
        return (uint64_t )sec;
      }

    } // end namespace link
  }
}

#endif
