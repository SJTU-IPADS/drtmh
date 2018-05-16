#include "tpcc_worker.h"
#include "tpcc_schema.h"
#include "tpcc_mixin.h"

#include "memstore/memdb.h"

#include "rdmaio.h"

#include "util/printer.h"

using namespace rdmaio;


extern size_t current_partition;
extern size_t nthreads;

namespace nocc {

  extern RdmaCtrl *cm;       // global RDMA handler

  namespace oltp {
    namespace tpcc {

      void populate_ware(MemDB *db) {
        char *temp = (char *)Rmalloc(256);
        for(uint wid = 1; wid <= NumWarehouses();++wid) {
          int pid;
          if( (pid = WarehouseToPartition(wid)) != current_partition) {
            // fetch it
            auto off = db->stores_[WARE]->RemoteTraverse(wid,cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
            assert(off != 0);

          } // end fetch
          Debugger::print_progress((double)wid / NumWarehouses());
        }   // end iterating all warehouses
        Rfree(temp);
      }

      void populate_dist(MemDB *db) {
        int pid;
        char *temp = (char *)Rmalloc(256);
        for(uint wid = 1;wid <= NumWarehouses();++wid) {
          for(uint d = 1; d <= NumDistrictsPerWarehouse();++d) {
            if( (pid = WarehouseToPartition(wid)) == current_partition) continue;
                uint64_t key = makeDistrictKey(wid,d);
                auto off = db->stores_[DIST]->RemoteTraverse(key,
                                                             cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
                assert(off != 0);
          } // iterating all districts
          Debugger::print_progress((double)wid / NumWarehouses());
        }
        Rfree(temp);
      }

      void populate_stock(MemDB *db) {

        char *temp = (char *)Rmalloc(256);
        int pid;
        for(uint wid = 1;wid <= NumWarehouses();++wid) {

          const size_t batchsize =  NumItems() ;
          const size_t nbatches = (batchsize > NumItems()) ? 1 : (NumItems() / batchsize);

          for (uint b = 0; b < nbatches;) {
            const size_t iend = std::min((b + 1) * batchsize + 1, NumItems());
            for (uint i = (b * batchsize + 1); i <= iend; i++) {

              uint64_t key = makeStockKey(wid,i);
              if((pid = WarehouseToPartition(wid)) == current_partition) continue;
              auto off = db->stores_[STOC]->RemoteTraverse(key,
                                                           cm->get_rc_qp(nthreads + nthreads + 1,pid,0),temp);
              assert(off != 0);
            }
            b++;
          } // end iterating all stocks
          Debugger::print_progress((double)wid / NumWarehouses());
        }
        Rfree(temp);
      }

    }; // namespace tpcc
  };   // namespace oltp

};     // namespace nocc
