#include "tpcc_worker.h"
#include "tpcc_schema.h"
#include "tpcc_mixin.h"
#include "tpcc_log_cleaner.h"
#include "tx_config.h"

#include <getopt.h>
#include <iostream>

#include "db/txs/dbsi.h"

#include "framework/bench_runner.h"

// for parsing config xml
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

using namespace std;

extern size_t scale_factor;
extern size_t nthreads;
extern size_t nclients;
extern uint64_t ops_per_worker;
extern size_t total_partition;

extern size_t nthreads;
extern volatile bool running;
extern int verbose;

extern size_t distributed_ratio;

namespace nocc {

  extern RdmaCtrl *cm;       // global RDMA handler
  namespace oltp {

    extern char *store_buffer; // the buffer used to store DrTM-kv

    namespace tpcc {

      /* parameters of TPCC */
      int g_uniform_item_dist = 0;
      int g_new_order_remote_item_pct = 1;
      int g_mico_dist_num = 20;
      // unsigned g_txn_workload_mix[5] = { 45, 43, 4, 4, 4 }; // default TPC-C workload mix
      unsigned g_txn_workload_mix[5] = { 0, 100, 0, 0, 0 }; // default TPC-C workload mix

      // remote loc cache related functions
      void populate_ware(MemDB *db);
      void populate_dist(MemDB *db);
      void populate_stock(MemDB *db);

      class TpccClient : public BenchClient {
      public:
        TpccClient(unsigned worker_id,unsigned seed,int total_wares)
          : BenchClient(worker_id,seed),
            total_wares_(total_wares){ }
        virtual int get_workload(char *input,util::fast_random &rand) {
          // pick execute machine
          int ware = (int)(rand.next_uniform() * (total_wares_) + 1);
          int pid  = WarehouseToPartition(ware);

          // pick tx idx
          static auto workload = TpccWorker::_get_workload();
          double d = rand.next_uniform();

          uint tx_idx = 0;
          for(size_t i = 0;i < workload.size();++i) {
            if((i + 1) == workload.size() || d < workload[i].frequency) {
              tx_idx = i;
              break;
            }
            d -= workload[i].frequency;
          }
          //fprintf(stdout,"TPCC pick %d, tx_idx %d\n",pid,tx_idx);
          *(uint8_t *)input = tx_idx;
          return pid;
        }
      private:
        int total_wares_;
      };

      class TpccMainRunner : public BenchRunner {
      public:
        TpccMainRunner(std::string &config_file) ;
        virtual void init_put();
        virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB* store = NULL);
        virtual std::vector<Worker *> make_workers();
        virtual std::vector<BackupBenchWorker *> make_backup_workers();
        virtual void init_store(MemDB* &store);
        virtual void init_backup_store(MemDB* &store);
        virtual void warmup_buffer(char *);
        virtual void populate_cache();

        virtual void bootstrap_with_rdma(RdmaCtrl *r) {
        }
      };

      void TpccTest(int argc, char **argv) {

        optind = 1;
        bool did_spec_remote_pct = false;
        while (1) {
          static struct option long_options[] =
            {
              {"new-order-remote-item-pct"            , required_argument , 0                                     , 'r'} ,
              {"micro-dist-num"                       , required_argument , 0                                     , 'n'} ,
              {"uniform-item-dist"                    , no_argument       , &g_uniform_item_dist                  , 1}   ,
              {"workload-mix"                         , required_argument , 0                                     , 'w'} ,
              {0, 0, 0, 0}
            };

          int option_index = 0;
          int c = getopt_long(argc, argv, "r:", long_options, &option_index);
          if (c == -1)
            break;
          switch (c) {
          case 0:
            if (long_options[option_index].flag != 0)
              break;
            abort();
            break;
          case 'r':
            g_new_order_remote_item_pct = strtoul(optarg, NULL, 10);
            ALWAYS_ASSERT(g_new_order_remote_item_pct >= 0 && g_new_order_remote_item_pct <= 100);
            did_spec_remote_pct = true;
            break;
          case 'n':
            g_mico_dist_num = strtoul(optarg,NULL,10);
            break;
          case 'w':
            {
              assert(false);
              const vector<string> toks = split(optarg, ',');
              ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
              unsigned s = 0;
              for (size_t i = 0; i < toks.size(); i++) {
                unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
                ALWAYS_ASSERT(p >= 0 && p <= 100);
                s += p;
                g_txn_workload_mix[i] = p;
              }
              ALWAYS_ASSERT(s == 100);
            }
            break;

          case '?':
            /* getopt_long already printed an error message. */
            exit(1);

          default:
            abort();
          }
        }

        if (verbose) {
          cerr << "[tpcc] settings:" << endl;
          cerr << "  new_order_remote_item_pct    : " << g_new_order_remote_item_pct << endl;
          cerr << "  uniform_item_dist            : " << g_uniform_item_dist << endl;
          cerr << "  micro dist :" << g_mico_dist_num << endl;
          //format_list(g_txn_workload_mix,
          //g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix)) << endl;
        }
        TpccMainRunner runner(nocc::oltp::config_file_name);
        runner.run();
        /* End TPCC bootstrap function */
      }

      /**/

      TpccMainRunner::TpccMainRunner(std::string &config_file)
        : BenchRunner(config_file) {

        // parse the TX mix ratio
        using boost::property_tree::ptree;
        using namespace boost;
        using namespace property_tree;

        ptree pt;
        bool init(true);

        // parse input xml
        try {
          read_xml(config_file,pt);
        } catch(const ptree_error &e) {
          init = false;
        }

        if(init) {
          // parse each TX's ratio
          try {
            int new_order = pt.get<int> ("bench.tpcc.new");
            int pay = pt.get<int> ("bench.tpcc.pay");
            int del = pt.get<int> ("bench.tpcc.del");
            int stock = pt.get<int> ("bench.tpcc.stock");
            int order = pt.get<int> ("bench.tpcc.order");

            g_txn_workload_mix[0] = new_order;
            g_txn_workload_mix[1] = pay;
            g_txn_workload_mix[2] = del;
            g_txn_workload_mix[3] = order;
            g_txn_workload_mix[4] = stock;

          } catch (const ptree_error &e) {
            // pass
          }

          // parse specific parameters
          try {
            //  g_new_order_remote_item_pct = pt.get<int> ("bench.tpcc.r");
            g_new_order_remote_item_pct = distributed_ratio;
          } catch (const ptree_error &e) {
            // pass
          }
        }

        workload_desc_vec_t workload = TpccWorker::_get_workload();
        for(uint i = 0;i < workload.size();++i) {
          cout << "Txn " << workload[i].name << ", "<< (workload[i].frequency * 100) << endl;
        }
        cout <<"Remote counts: " << g_new_order_remote_item_pct << endl;
        cout <<"NAIVE: " << NAIVE;

      }

      void TpccMainRunner::init_store(MemDB* &store){

        store = new MemDB(store_buffer);
        int meta_size = META_SIZE;


#if ONE_SIDED_READ == 0
        // store as normal B-tree
        store->AddSchema(WARE,TAB_BTREE,sizeof(uint64_t),sizeof(warehouse::value),meta_size);
        store->AddSchema(DIST,TAB_BTREE,sizeof(uint64_t),sizeof(district::value),meta_size);
        store->AddSchema(STOC,TAB_BTREE,sizeof(uint64_t),sizeof(stock::value),meta_size);
#else
        assert(scale_factor > 0);
        // store as DrTM kv
        store->AddSchema(WARE,TAB_HASH,sizeof(uint64_t),sizeof(warehouse::value),meta_size,scale_factor * 2);
        store->AddSchema(DIST,TAB_HASH,sizeof(uint64_t),sizeof(district::value),meta_size,
                         scale_factor * NumDistrictsPerWarehouse() * 2);
        store->AddSchema(STOC,TAB_HASH,sizeof(uint64_t),sizeof(stock::value),meta_size,
                         NumItems() * scale_factor * 2);

        store->EnableRemoteAccess(WARE,cm);
        store->EnableRemoteAccess(DIST,cm);
        store->EnableRemoteAccess(STOC,cm);
#endif
        store->AddSchema(CUST,TAB_BTREE,sizeof(uint64_t),sizeof(customer::value),meta_size);
        store->AddSchema(HIST,TAB_BTREE,sizeof(uint64_t),sizeof(history::value),meta_size);
        store->AddSchema(NEWO,TAB_BTREE,sizeof(uint64_t),sizeof(new_order::value),meta_size);
        store->AddSchema(ORDE,TAB_BTREE,sizeof(uint64_t),sizeof(oorder::value),meta_size);
        store->AddSchema(ORLI,TAB_BTREE,sizeof(uint64_t),sizeof(order_line::value),meta_size);
        store->AddSchema(ITEM,TAB_BTREE,sizeof(uint64_t),sizeof(item::value),meta_size);

        // secondary index
        //store->AddSchema(CUST_INDEX,TAB_SBTREE,sizeof(uint64_t),16,meta_size);
        //      store->AddSchema(ORDER_INDEX,TAB_BTREE,sizeof(uint64_t),16,meta_size);
        store->AddSchema(CUST_INDEX,TAB_BTREE1, 5,16,meta_size);
        store->AddSchema(ORDER_INDEX,TAB_BTREE, sizeof(uint64_t),16,meta_size);

      }

      void TpccMainRunner::init_backup_store(MemDB* &store){
        store = new MemDB();
        int meta_size = META_SIZE;

        // store as DrTM kv
        store->AddSchema(WARE,TAB_HASH,sizeof(uint64_t),sizeof(warehouse::value),meta_size);
        store->AddSchema(DIST,TAB_HASH,sizeof(uint64_t),sizeof(district::value),meta_size);
        store->AddSchema(STOC,TAB_HASH,sizeof(uint64_t),sizeof(stock::value),meta_size);

        store->AddSchema(CUST,TAB_BTREE,sizeof(uint64_t),sizeof(customer::value),meta_size);
        store->AddSchema(HIST,TAB_BTREE,sizeof(uint64_t),sizeof(history::value),meta_size);
        store->AddSchema(NEWO,TAB_BTREE,sizeof(uint64_t),sizeof(new_order::value),meta_size);
        store->AddSchema(ORDE,TAB_BTREE,sizeof(uint64_t),sizeof(oorder::value),meta_size);
        store->AddSchema(ORLI,TAB_BTREE,sizeof(uint64_t),sizeof(order_line::value),meta_size);
        store->AddSchema(ITEM,TAB_BTREE,sizeof(uint64_t),sizeof(item::value),meta_size);

        // secondary index
        //store->AddSchema(CUST_INDEX,TAB_SBTREE,sizeof(uint64_t),16,meta_size);
        //      store->AddSchema(ORDER_INDEX,TAB_BTREE,sizeof(uint64_t),16,meta_size);
        store->AddSchema(CUST_INDEX,TAB_BTREE1, 5,16,meta_size);
        store->AddSchema(ORDER_INDEX,TAB_BTREE, sizeof(uint64_t),16,meta_size);
      }


      void TpccMainRunner::init_put() {

        uint64_t *temp = new uint64_t[4];
        temp[0] = 0; temp[1] = 0; temp[2] = 0;
        for (int i=0; i < 9; i++) {
          //Fixme: invalid value pointer
          store_->Put(i,(uint64_t)1<<60, temp);
        }
        //XXX: add empty record to identify the end of the table./
        store_->Put(ORDER_INDEX,(uint64_t)1<<60, temp);

        //FIXME, do we need to add a last value for the customer secondary index ?
      }

      std::vector<BenchLoader *> TpccMainRunner::make_loaders(int partition, MemDB* store) {

        fprintf(stdout,"[TPCC loader] total %d warehouses\n",NumWarehouses());
        /* Current we do not consider parallal loading */
        std::vector<BenchLoader *> ret;
        if(store == NULL){
          ret.push_back(new TpccWarehouseLoader(9324,partition, store_));
          ret.push_back(new TpccItemLoader(235443,partition,store_));
          ret.push_back(new TpccStockLoader(89785943,partition,store_));
          ret.push_back(new TpccDistrictLoader(129856349,partition,store_));
          ret.push_back(new TpccCustomerLoader(923587856425,partition,store_));
          ret.push_back(new TpccOrderLoader(2343352,partition,store_));
        } else {
          // ret.push_back(new TpccWarehouseLoader(9324,partition, store));
          // ret.push_back(new TpccItemLoader(235443,partition,store));
          ret.push_back(new TpccStockLoader(89785943,partition,store));
          ret.push_back(new TpccDistrictLoader(129856349,partition,store));
          // ret.push_back(new TpccCustomerLoader(923587856425,partition,store));
          // ret.push_back(new TpccOrderLoader(2343352,partition,store));
        }
        return ret;
      }

      std::vector<Worker *> TpccMainRunner::make_workers() {

        fast_random r(23984543 + current_partition);

        std::vector<Worker *> ret;

        int n_ware_per_worker = scale_factor / nthreads;
        fprintf(stdout, "[Tpcc] n_ware_per_worker: %d\n", n_ware_per_worker);
        assert(n_ware_per_worker > 0);
        for(uint i = 0;i < nthreads; ++i) {
          //  assert(ops_per_worker > 0);
          if(n_ware_per_worker == 0) {
            ret.push_back(new TpccWorker(i,r.next(),current_partition + 1,
                                         current_partition + 2,
                                         store_,ops_per_worker,
                                         &barrier_a_,&barrier_b_,this));
          } else {
            // TODO, current donot consider cases when one threads holds more warehouses
            ret.push_back(new TpccWorker(i,r.next(),current_partition * scale_factor + i + 1,
                                         current_partition * scale_factor + i + n_ware_per_worker + 1 ,
                                         store_,ops_per_worker,
                                         &barrier_a_,&barrier_b_,this));
          }
        }

#if SI_TX
        // add ts worker
        ts_manager = new TSManager(nthreads + nclients + 1,cm,0,0);
        ret.push_back(ts_manager);
#endif

#if CS == 1
        for(uint i = 0;i < nclients;++i)
          ret.push_back(new TpccClient(nthreads + i,r.next(),n_ware_per_worker * nthreads * total_partition));
#endif


        return ret;
      }

      std::vector<BackupBenchWorker *> TpccMainRunner::make_backup_workers() {
        std::vector<BackupBenchWorker *> ret;

        int num_backups = my_view->is_backup(current_partition);
        LogCleaner* log_cleaner = new TpccLogCleaner;
        for(uint j = 0; j < num_backups; j++){
          assert(backup_stores_[j] != NULL);
          log_cleaner->add_backup_store(backup_stores_[j]);
        }

        DBLogger::set_log_cleaner(log_cleaner);
        for(uint i = 0; i < backup_nthreads; i++){
          ret.push_back(new BackupBenchWorker(i));
        }
        return ret;
      }

      void TpccMainRunner::warmup_buffer(char *buffer) {
      }

      void TpccMainRunner::populate_cache() {
#if CACHING == 1
        // create a temporal QP for usage
        int dev_id = cm->get_active_dev(0);
        int port_idx = cm->get_active_port(0);

        cm->thread_local_init();
        cm->open_device(dev_id);
        cm->register_connect_mr(dev_id); // register memory on the specific device

        cm->link_connect_qps(nthreads + nthreads + 1,dev_id,port_idx,0,IBV_QPT_RC);

        // calculate the time of populating the cache
        struct  timeval start;
        struct  timeval end;

        gettimeofday(&start,NULL);

        //populate_ware(store_);
        //populate_dist(store_);
        populate_stock(store_);

        gettimeofday(&end,NULL);

        auto diff = (end.tv_sec-start.tv_sec) + (end.tv_usec - start.tv_usec) /  1000000.0;
        fprintf(stdout,"Time to loader the caching is %f second\n",diff);
#endif
      }

      int GetStartWarehouse(int partition) {
        return partition * scale_factor  + 1;
      }

      int GetEndWarehouse(int partition) {
        return (partition + 1) * scale_factor;
      }

      int NumWarehouses() {
        return (int)(scale_factor * total_partition);
      }

      int WarehouseToPartition(int wid) {
        return (wid - 1) / scale_factor;
      }
      /* end namespace tpcc */
    }


  }
}
