#include "tx_config.h"

#include "bench_micro.h"
#include "framework/req_buf_allocator.h"
#include <iostream>

#include "util/mapped_log.h"

#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"

#include "framework/bench_runner.h"

#include "../smallbank/bank_worker.h" // use the smallbank workload for test

// for parsing config xml
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

using namespace std;

// global configure parameters
extern size_t scale_factor;
extern size_t nthreads;
extern uint64_t ops_per_worker;
extern size_t total_partition;
extern size_t distributed_ratio;

using namespace rdmaio;

extern __thread MappedLog local_log;

namespace nocc {

extern __thread RPCMemAllocator *msg_buf_alloctors;
using namespace util;

namespace oltp {

extern char *rdma_buffer;      // start point of the local RDMA registered buffer
extern char *free_buffer;      // start point of the local RDMA heap. before are reserved memory
extern uint64_t r_buffer_size; // total registered buffer size

extern __thread util::fast_random   *random_generator;

Breakdown_Timer *send_req_timers;
Breakdown_Timer *compute_timers;

namespace micro {

int micro_type;
uint64_t working_space = 8 * 1024 * 1024;

unsigned g_txn_workload_mix[1] = { 100 }; // default TPC-C workload mix

char *test_buf = NULL; // used for thread local write to emulate local store

class MicroMainRunner : public BenchRunner {
  public:
	MicroMainRunner(std::string &config_file) : BenchRunner(config_file){

		// parse the TX mix ratio
		using boost::property_tree::ptree;
		using namespace boost;
		using namespace property_tree;
		ptree pt;
		bool init(true);

		read_xml(config_file,pt);

		// parse input xml
		try {
			int type = pt.get<int>("bench.micro");
			micro_type = type;

		} catch(const ptree_error &e) {
			assert(false);
		}

		try {
			working_space = pt.get<uint64_t>("bench.space");
			working_space = working_space * (1024 * 1024);
			fprintf(stdout,"working space %f\n",get_memory_size_g(working_space));
		} catch(const ptree_error &e) {

		}
	} // constructer of MicroMainRunner

	virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB* store){
		return std::vector<BenchLoader *>();
	}
	virtual std::vector<BackupBenchWorker *> make_backup_workers() {
		return std::vector<BackupBenchWorker *> ();
	}
	virtual void init_store(MemDB* &store) { store = new MemDB(); assert(store_ != NULL);}
	virtual void init_backup_store(MemDB* &store) {}
	virtual std::vector<RWorker *> make_workers();
	virtual void warmup_buffer(char *ptr) {
	}

	virtual void bootstrap_with_rdma(RdmaCtrl *cm) {
	}

	virtual void init_put() {

		if(micro_type != MICRO_TX_RAD && micro_type != MICRO_TX_RW && micro_type != MICRO_TX_READ)
			return; // only test TX workloads
		assert(store_ != NULL);
		int meta_size = META_SIZE;
		store_->AddSchema(TAB,TAB_HASH,sizeof(uint64_t),CACHE_LINE_SZ,META_SIZE);
		using namespace nocc::oltp::bank;

		scale_factor = 1;
		fprintf(stdout,"[MICRO] %d accounts total loaded\n",NumAccounts());

		uint64_t account_per_mac = NumAccounts() / total_partition;

		fast_random r;
		r.set_seed0(current_partition * 73 + 1);
#if 1
		for(uint64_t i = account_per_mac * current_partition,k(0);
			k < account_per_mac;++i,++k){

			char *wrapper_acct = new char[meta_size + CACHE_LINE_SZ];
			assert(wrapper_acct != NULL);
			memset(wrapper_acct,0,meta_size);
			store_->Put(TAB,i,(uint64_t *)wrapper_acct);


			// set a random seq
#if SI_TX
			MemNode *node = store_->stores_[TAB]->GetWithInsert(i);
			uint64_t id = r.next() % total_partition;
			node->seq = SI_ENCODE_TS(id,2);
#endif
		}
#else
		for(uint i = 0;i < NumAccounts();++i) {
			int pid = AcctToPid(i);
			if(pid != current_partition) continue;
			char *wrapper_acct = new char[meta_size + CACHE_LINE_SZ];
			assert(wrapper_acct != NULL);
			memset(wrapper_acct,0,meta_size);
			store_->Put(TAB,i,(uint64_t *)wrapper_acct);
		}
#endif
	}

};

void MicroTest(int argc,char **argv) {
	MicroMainRunner runner(nocc::oltp::config_file_name);
	runner.run();
}

MicroWorker::MicroWorker(unsigned int worker_id,unsigned long seed,int micro_type,MemDB *store,
						 uint64_t total_ops, spin_barrier *a,spin_barrier *b,BenchRunner *r):
		BenchWorker(worker_id,true,seed,total_ops,a,b,r),
		store_(store)
{

	uint64_t free_offset = free_buffer - rdma_buffer;
	uint64_t total_free  = r_buffer_size - free_offset;

	if(test_buf == NULL)
		test_buf = (char *)malloc(working_space);
	INIT_LAT_VARS(post);
	for(uint i = 0;i < 16;++i) heatmap[i] = 0;
}

void MicroWorker::thread_local_init() {

	switch (micro_type) {
		case MICRO_RPC_SCALE: {
			reply_buf_ = (char *)malloc(1024 * 8);
			break;
		}
		case MICRO_RDMA_SCALE: {
			qps = new struct ibv_qp*[total_partition];
			cqs = new struct ibv_cq*[total_partition];
			mrs = new struct ibv_mr*[total_partition];

			addrs = new uint64_t[total_partition];
			rkeys = new uint64_t[total_partition];
			break;
		}
		case MICRO_RPC_STRESS: {
			send_req_timers = new Breakdown_Timer();
			compute_timers  = new Breakdown_Timer();
			reply_buf_ = (char *)malloc(1024 * 16);
			break;
		}
		case MICRO_RDMA_ATOMIC_MULTI:
		case MICRO_RDMA_ATOMIC:
		case MICRO_RDMA_WRITE:
		case MICRO_RDMA_READ:
		case MICRO_RDMA_WRITE_MULTI:
		case MICRO_RDMA_RW:
		case MICRO_RDMA_READ_MULTI:
		case MICRO_RDMA_SCHED:
			{
				// connecting QPs
				for(uint i = 0;i < cm_->get_num_nodes();++i) {
					Qp *qp = cm_->get_rc_qp(worker_id_,i,0);
					//Qp *qp = cm_->get_rc_qp(worker_id_ + 8,i,1);
					qps_.push_back(qp);
				}
				rdma_buf_ = (char*)Rmalloc(4096);
				break;
			}
		case MICRO_LOGGER_WRITE: {
			assert(db_logger_ != NULL);
			break;
		}
		case MICRO_TS_STRSS:
		case MICRO_TX_RAD:
		case MICRO_TX_RW:
		case MICRO_TX_READ:
			{
				// init tx data structures
				for(uint i = 0;i < server_routine + 1;++i) {
#ifdef RAD_TX
					txs_[i] = new DBRad(store_,worker_id_,rpc_,i);
#elif defined(OCC_TX)
					txs_[i] = new DBTX(store_,worker_id_,rpc_,i);
#elif defined(FARM)
					txs_[i] = new DBFarm(cm,rdma_sched_,store_,worker_id_,rpc_,i);
#elif defined(SI_TX)
					txs_[i] = new DBSI(store_,worker_id_,rpc_,i);
#else
					ASSERT_PRINT(false,stdout,"No transactional layer used.\n");
#endif
				} // end init tx handlers for coroutines
				tx_ = txs_[cor_id_];
				// connecting QPs
#if 0
				for(uint i = 0;i < cm_->get_num_nodes();++i) {
					Qp *qp = cm_->get_rc_qp(worker_id_,i,1);
					qps_.push_back(qp);
				}
#endif

			}
			break;
		default:
			assert(false);
			break;
	}
	// init the reply bufs
	reply_bufs_ = new char *[server_routine + 1];
	for(uint i = 0; i <= server_routine;++i) {
		reply_bufs_[i] = (char *)malloc(4096); assert(reply_bufs_[i] != NULL); }

}

void MicroWorker::register_callbacks() {

	fprintf(stdout,"[MICRO]: register callbacks\n");
	switch(micro_type) {
		case MICRO_RPC_STRESS:
			rpc_->register_callback(boost::bind(&MicroWorker::nop_rpc_handler,this,_1,_2,_3,_4),
									RPC_NOP);
			rpc_->register_callback(boost::bind(&MicroWorker::null_rpc_handler,this,_1,_2,_3,_4),
									RPC_NULL);
			break;
		case MICRO_RDMA_READ_MULTI:
#if NAIVE == 73
			rpc_->register_callback(boost::bind(&MicroWorker::batch_read_rpc_handler,this,_1,_2,_3,_4),
									RPC_BATCH_READ);
#else
			rpc_->register_callback(boost::bind(&MicroWorker::read_rpc_handler,this,_1,_2,_3,_4),
									RPC_READ);
#endif
			break;
		case MICRO_RDMA_READ:
			rpc_->register_callback(boost::bind(&MicroWorker::various_read_rpc_handler,
												this,_1,_2,_3,_4),RPC_READ);
			break;
		case MICRO_RDMA_WRITE:
			rpc_->register_callback(boost::bind(&MicroWorker::write_rpc_handler,
												this,_1,_2,_3,_4),RPC_WRITE);
			break;
		case MICRO_RDMA_WRITE_MULTI:
			rpc_->register_callback(boost::bind(&MicroWorker::write_rpc_handler,this,_1,_2,_3,_4),RPC_WRITE);
			rpc_->register_callback(boost::bind(&MicroWorker::batch_write_rpc_handler,
												this,_1,_2,_3,_4),RPC_BATCH_WRITE);
			rpc_->register_callback(boost::bind(&MicroWorker::batch_read_rpc_handler,
												this,_1,_2,_3,_4),RPC_BATCH_READ); // for test only!

			rpc_->register_callback(boost::bind(&MicroWorker::read_rpc_handler,
												this,_1,_2,_3,_4),RPC_READ); // for test only!
			break;
		case MICRO_TX_RAD:
			RoutineMeta::register_callback(boost::bind(&MicroWorker::tx_one_shot_handler,this,_1,_2,_3,_4),RPC_READ);
			rpc_->register_callback(boost::bind(&MicroWorker::tx_one_shot_handler2,
												this,_1,_2,_3,_4),RPC_READ);
			break;
		case MICRO_TX_RW:
			RoutineMeta::register_callback(boost::bind(&MicroWorker::tx_one_shot_handler,this,_1,_2,_3,_4),RPC_READ);
			break;
		case MICRO_TX_READ:
			rpc_->register_callback(boost::bind(&MicroWorker::tx_read_handler,
												this,_1,_2,_3,_4),RPC_READ);
			rpc_->register_callback(boost::bind(&MicroWorker::tx_write_ts,
												this,_1,_2,_3,_4),RPC_READ + 1);
			rpc_->register_callback(boost::bind(&MicroWorker::tx_ro_handler,
												this,_1,_2,_3,_4),RPC_READ_ONLY);
			break;
		default:
			// pass
			break;
	}

}
void MicroWorker::workload_report() {
	REPORT(post);
}


workload_desc_vec_t MicroWorker::get_workload() const {
	return _get_workload();
}

workload_desc_vec_t MicroWorker::_get_workload() {

	workload_desc_vec_t w;
	unsigned m = 0;

	string name;
	string name1;

	txn_fn_t fn;
	txn_fn_t fn1;

	switch (micro_type) {
		case MICRO_RPC_SCALE: {
			name = "RpcScale";        fn = MicroRpcScale;
			break;
		}
		case MICRO_RDMA_SCALE: {
			name = "RdmaScale";       fn = MicroRdmaScale;
			break;
		}
		case MICRO_RDMA_DOORBELL_SCALE: {
			name = "RdmaScale";       fn = MicroRdmaDoorbellScale;
			break;
		}
		case MICRO_RPC_STRESS: {
			name = "RpcTest";         fn = MicroRpcStress;
			break;
		}
		case MICRO_LOGGER_FUNC: {
			name = "LOGGER_FUNC";     fn = MicroLoggerFunc;
			break;
		}
		case MICRO_RDMA_SCHED: {
			name = "RDMA_SCHED";      fn = MicroRDMASched;
			break;
		}
		case MICRO_RDMA_RW: {
			name = "RDMA_ONE_RW"; fn = MicroRdmaOneRW;
			break;
		}
		case MICRO_RDMA_READ_MULTI: {
#if RPC == 1
			name = "RPC multi read";  fn = MicroRPCMulti;
#else
			name = "RDMA_MULTI_READ"; fn = MicroRdmaMulti;
#endif
			break;
		}
		case MICRO_RDMA_WRITE_MULTI:{
#if RPC == 1
			name = "RPC write multi" ; fn = MicroRPCMultiWrite;
#else
			name = "RDMA write multi"; fn = MicroRdmaMultiWrite;
#endif
			break;
		}
		case MICRO_RDMA_WRITE: {
#if RPC == 1
			name = "RPC write" ; fn = MicroRPCWrite;
#else
			name = "RDMA write"; fn = MicroRDMAWrite;
#endif
			break;
		}
		case MICRO_LOGGER_WRITE: {
			name = "RPC_LOGGER_WRITE"; fn = MicroLoggerWrite;
			break;
		}
		case MICRO_RPC_READ: {
			name = "RPC_READ_VARIOUS"; fn = MicroRPCRead;
			break;
		}
		case MICRO_RDMA_READ: {
#if RPC == 1
			name = "RDMA_RPC Read various"; fn = MicroRPCRead;
#else
			name = "RDMA read various";fn = MicroRDMARead;
#endif
			break;
		}
		case MICRO_RDMA_ATOMIC:{
			name = "RDMA ATOMIC";fn = MicroRDMAAtomic;
			break;
		}
		case MICRO_RDMA_ATOMIC_MULTI:{
			name = "RDMA Atomic multi";fn = MicroRDMAMultiAtomic;
			break;
		}
		case MICRO_TS_STRSS: {
			name = "TX timestamp stress";fn = MicroTXTs;
			break;
		}
		case MICRO_TX_RAD: {
			name = "TX rad stress"; fn = MicroTXRad;
			break;
		}
		case MICRO_TX_RW: {
			name = "TX read/write"; fn = MicroTXRW;
		}
			break;
		case MICRO_TX_READ:{
			//name = "TX read test"; fn = MicroTXRead;
			// a special case
			w.push_back(workload_desc("TX read test",100.0 / 100.0,MicroTXRead));
			//w.push_back(workload_desc("TX read test",distributed_ratio / 100.0,MicroTXRead));
			//w.push_back(workload_desc("TX rw test",(100 - distributed_ratio) / 100.0,MicroTXRW));
			return w;
		}
			break;
		default:
			assert(false);
	}
	w.push_back(workload_desc(name,double(g_txn_workload_mix[0]) / 100.0,fn));
	return w;
}

std::vector<RWorker *> MicroMainRunner::make_workers() {
	fast_random r(23984543 + current_partition);
	std::vector<RWorker *> ret;
	for(uint i = 0;i < nthreads; ++i) {
		ret.push_back(new MicroWorker(i,r.next(),micro_type,store_,ops_per_worker,
									  &barrier_a_,&barrier_b_,
									  static_cast<BenchRunner *>(this)));
	}
#if SI_TX
	// add ts worker
	ts_manager = new TSManager(nthreads,cm,0,0);
	ret.push_back(ts_manager);
#endif

	return ret;
}
} // end namespace micro
} // end namespace oltp
} //end namespace nocc
