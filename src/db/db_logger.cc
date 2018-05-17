#include "db_logger.h"
#include "framework/bench_worker.hpp"
#include "framework/req_buf_allocator.h"

extern size_t nthreads;
extern size_t current_partition;

namespace nocc {

	namespace oltp {
		extern __thread oltp::RPCMemAllocator *msg_buf_alloctors;
	}
	using namespace oltp;

	extern __thread BenchWorker* worker;

	namespace db {

		LogCleaner* DBLogger::log_cleaner_ = NULL;
		uint64_t DBLogger::base_offset_  = 0;

#if TX_LOG_STYLE == 0
		DBLogger::DBLogger(int thread_id,RdmaCtrl *rdma,View *view, RDMA_sched* rdma_sched)
			: rdma_(rdma), view_(view), thread_id_(thread_id), rdma_sched_(rdma_sched) {
#elif TX_LOG_STYLE == 1
		DBLogger::DBLogger(int thread_id,RdmaCtrl *rdma,View *view,RRpc *rpc_handler)
			: rdma_(rdma), view_(view), thread_id_(thread_id), rpc_handler_(rpc_handler) {
#elif TX_LOG_STYLE == 2
		DBLogger::DBLogger(int thread_id,RdmaCtrl *rdma,View *view, RDMA_sched* rdma_sched, RRpc *rpc_handler)
			: rdma_(rdma), view_(view), thread_id_(thread_id), rdma_sched_(rdma_sched), rpc_handler_(rpc_handler) {
#endif
			assert(rdma != NULL);

			num_nodes_ = rdma_->get_num_nodes();
			//computer the start location
		  	//DZY: conn_buf_ is the start pointer of our memory region
		  	base_ptr_ = (char*)rdma_->conn_buf_ + base_offset_;
		  	node_area_size_ = THREAD_AREA_SIZE * nthreads;
		  	translate_offset_ = base_offset_ + current_partition * node_area_size_
		  								+ thread_id_ * THREAD_AREA_SIZE + THREAD_META_SIZE;

		  	local_meta_ptr_ = base_ptr_ + current_partition * node_area_size_
		  								+ thread_id_ * THREAD_AREA_SIZE;
		  	base_local_buf_ptr_ = (char*)rdma_->conn_buf_;

		  	// allocate temprary logs for each coroutine
		  	temp_logs_ = new TempLog*[worker->server_routine + 1];
		  	for(int i = 1; i <= worker->server_routine; i++){
#if TX_LOG_STYLE > 0
		  		temp_logs_[i] = new TempLog(num_nodes_, num_nodes_ * sizeof(DBLogger::ReplyHeader),
		  			sizeof(uint64_t) + sizeof(rpc_header) + sizeof(DBLogger::RequestHeader));
#else
		  		temp_logs_[i] = new TempLog(num_nodes_, num_nodes_ * sizeof(char));
#endif
		  	}

		  	// initialize meta data of each remote ring buffer
		  	remain_sizes_ = new int[num_nodes_];
		  	remote_offsets_ = new uint64_t[num_nodes_];
		  	for(int i = 0; i < num_nodes_; i++){
		  		remain_sizes_[i] = THREAD_BUF_SIZE;
		  		remote_offsets_[i] = 0;
		  	}

		}

		DBLogger::~DBLogger() {

		  for(uint i = 1; i <= worker->server_routine; i++){
			  delete temp_logs_[i];
		  }
		  delete [] temp_logs_;
		  delete [] remain_sizes_;
		  delete [] remote_offsets_;
		}

		void DBLogger::thread_local_init(){
#if TX_LOG_STYLE == 1
			rpc_handler_->register_callback(std::bind(&DBLogger::logging_handler,this,
                                                  std::placeholders::_1,
                                                  std::placeholders::_2,
                                                  std::placeholders::_3,
                                                  std::placeholders::_4),RPC_LOGGING);
#endif
#if TX_LOG_STYLE > 0
			rpc_handler_->register_callback(std::bind(&DBLogger::logging_commit_handler,this,
                                                  std::placeholders::_1,
                                                  std::placeholders::_2,
                                                  std::placeholders::_3,
                                                  std::placeholders::_4),RPC_LOGGING_COMMIT);
#endif
#if TX_LOG_STYLE == 0 || TX_LOG_STYLE == 2
			assert(rdma_sched_ != NULL);

			memset(qp_idx_,0,sizeof(int)*16);

			for(uint i = 0;i < num_nodes_;++i) {
				for(uint j = 0;j < QP_NUMS ; j++){
					Qp *qp = rdma_->get_rc_qp(thread_id_,i,j);
					assert(qp != NULL);
					qp_vec_.push_back(qp);
				}
			}
#endif
		}

		void DBLogger::log_begin(uint cor_id, uint64_t global_seq){
			//assert(cor_id >= 1 && cor_id <= coroutine_num);
			// printf("log_begin: cor_id:%d\n", cor_id);

			TempLog *temp_log = temp_logs_[cor_id];
			temp_log->open();

			// XD: shall remove this line
			// DZY: for smallbank, this line should be removed
			//      however, for tpcc, it can not be removed due to some bugs, currently.
			view_.add_backup(current_partition, temp_log->mac_backups_);
			TXHeader *header = (TXHeader*)temp_log->append_entry(sizeof(TXHeader));
			// reserved operation for future TXHeader fields,
			header->magic_num = LOG_HEADER_MAGIC_NUM;
			header->global_seq = global_seq;
			temp_log->close_entry();
		}

		// temprarily we think remote_id as partition id
		char* DBLogger::get_log_entry(uint cor_id, int table_id, uint64_t key, uint32_t size, int partition_id)
		{
			TempLog *temp_log = temp_logs_[cor_id];

			EntryMeta* entry_meta = (EntryMeta*)temp_log->append_entry(sizeof(EntryMeta) + size);
			entry_meta->table_id = table_id;
			entry_meta->size 		= size;
			entry_meta->key 		= key;

			// the data is in the remote
			// we assume that local log is in NVRAM
			// so nothing to do for local data
			if(unlikely(partition_id >= 0)){
				auto& partitions = temp_log->partitions_;
				auto& mac_backups = temp_log->mac_backups_;
				// put all partition_id's backup mac_id to mac_backups_
				view_.add_backup(partition_id, mac_backups);
				if(unlikely(partitions.find(partition_id) == partitions.end())){
					partitions.insert(partition_id);
					mac_backups.insert(view_.partition_to_mac(partition_id));
				}
			}
			return temp_log->current_ + sizeof(EntryMeta);
		}

		void DBLogger::close_entry(uint cor_id, uint64_t seq){

			TempLog *temp_log = temp_logs_[cor_id];

			EntryMeta* entry_meta = (EntryMeta*)temp_log->get_current_ptr();
			entry_meta->seq = seq;
			// print_log_entry(temp_log->current_);
			temp_log->close_entry();
		}

		int DBLogger::log_backups(uint cor_id, uint64_t seq){

			int ret = LOG_SUCC;

			TempLog *temp_log = temp_logs_[cor_id];

			temp_log->log_size_round_up();

			TXTailer *tailer = (TXTailer*)temp_log->append_entry(sizeof(TXTailer));
			tailer->magic_num = LOG_TAILER_MAGIC_NUM;
			tailer->seq = seq;
			temp_log->close_entry();

			ret = log_setup(cor_id);
			assert(ret == LOG_SUCC);
			temp_log->pending_acks_num_ = temp_log->remote_mac_num_;
			
			uint64_t* log_size_ptr = (uint64_t *)temp_log->start_;
			*log_size_ptr = temp_log->get_log_size();
		  	if(temp_log->mac_backups_.size() == 0){
				// printf("no backups!-------------------------\n");
				return ret;
			}
			int length = temp_log->get_total_size();
			// printf("total_size:%d\n", length);

#if TX_LOG_STYLE == 1
			DBLogger::RequestHeader* request_header =
				(DBLogger::RequestHeader*)(temp_log->start_ - sizeof(DBLogger::RequestHeader));
            request_header->length = length;
            memcpy(request_header->offsets, temp_log->remote_log_offsets_, sizeof(uint64_t) * num_nodes_);

            //rpc_handler_->set_msg((char*) request_header);
            //rpc_handler_->send_reqs(RPC_LOGGING,length + sizeof(DBLogger::RequestHeader), temp_log->remote_mac_reps_,
			//                  temp_log->remote_mac_ids_,temp_log->remote_mac_num_,cor_id);
			//int *log_macs = new int[2];
			//int  num_logs = 0;
			//num_logs = view_.get_backup(current_partition,log_macs);

			assert(temp_log->remote_mac_num_ > 0);
			//rpc_handler_->prepare_multi_req(temp_log->remote_mac_reps_,temp_log->remote_mac_num_,cor_id);
			rpc_handler_->prepare_multi_req(temp_log->remote_mac_reps_,num_logs,cor_id);
			rpc_handler_->broadcast_to((char *)request_header,RPC_LOGGING,length + sizeof(DBLogger::RequestHeader),
									   cor_id,RRpc::REQ,temp_log->remote_mac_ids_,temp_log->remote_mac_num_);
									   //log_macs,num_logs);

#else
			//if(temp_log->remote_mac_num_ > 3){
			//	fprintf(stdout,"remote log num %d\n",temp_log->remote_mac_num_);
			//assert(false);
			//}
			for(int i = 0; i < temp_log->remote_mac_num_; i++){
				int remote_id = temp_log->remote_mac_ids_[i];
				// Qp* qp = qp_vec_[remote_id];
				Qp* qp = get_qp(remote_id);
				// printf("remote_mac:%d, log_offset:%lu, length:%d\n", remote_id,temp_log->remote_log_offsets_[remote_id],length);
				qp->rc_post_send(IBV_WR_RDMA_WRITE,temp_log->start_,length,
					temp_log->remote_log_offsets_[remote_id],IBV_SEND_SIGNALED,cor_id);
				rdma_sched_->add_pending(cor_id,qp);
				// qp->poll_completion();
			}
#endif
			// printf("log_backups: cor_id:%u\n", cor_id);
			return ret;
		}

		int DBLogger::log_setup(uint cor_id){
			int ret = LOG_SUCC;

			TempLog *temp_log = temp_logs_[cor_id];

			uint64_t log_total_size = temp_log->get_total_size();
			// printf("total_size:%lu\n", log_total_size);
			assert(log_total_size <= THREAD_PADDING_SIZE);

			for(auto backup = temp_log->mac_backups_.begin(); backup != temp_log->mac_backups_.end();
				  backup++, temp_log->remote_mac_num_++){
				int remote_id = *backup;
				// printf("log_setup: remote_id: %d\n",remote_id);

				temp_log->remote_mac_ids_[temp_log->remote_mac_num_] = remote_id;
				int whole_temp_log_size = log_total_size + sizeof(uint64_t);
				// printf("whole_temp_log_size : %d\n", whole_temp_log_size);
#if USE_BACKUP_STORE && USE_BACKUP_BENCH_WORKER
				if(remain_sizes_[remote_id] < whole_temp_log_size + LOG_HOLE){
					volatile uint64_t *ptr = (volatile uint64_t*)(local_meta_ptr_) + remote_id;
					uint64_t remain_log_size,count = 0;

					do{
						uint64_t remote_offset = remote_offsets_[remote_id]; 
						volatile uint64_t remote_header = *ptr;

			      		/* following code is used for debugging*/
			      		// volatile uint64_t remote_header = remote_offset;

						// for ease !!!!!!!!!
			      		assert(remote_header < THREAD_BUF_SIZE);

			      		// BUG FIXED, use greater(>) rather than not less(>=)
			      		//                               |
			      		//                               V
			      		remain_log_size = (remote_offset > remote_header) ?
			      									(THREAD_BUF_SIZE - remote_offset + remote_header ) :
			      									(remote_header - remote_offset);
						/* following code is used for debugging*/
			      		// remain_log_size = THREAD_BUF_SIZE;

			      	} while (remain_log_size < whole_temp_log_size + LOG_HOLE);

			      	remain_sizes_[remote_id] = remain_log_size;
				}
				// printf("remain_size: %d \n",remain_sizes_[remote_id]);
				remain_sizes_[remote_id] -= whole_temp_log_size;
				// printf("remain_size: %d \n",remain_sizes_[remote_id]);
#endif
				uint64_t remote_offset = remote_offsets_[remote_id];
				// printf("remote_offset: %lx\n",remote_offset);


				temp_log->remote_log_offsets_[remote_id] = translate_offset(remote_offset);
				temp_log->remote_ack_offsets_[remote_id] = translate_offset(remote_offset + log_total_size);

				remote_offsets_[remote_id] = (remote_offset + whole_temp_log_size) % THREAD_BUF_SIZE;
			}

			return ret;
		}

		int DBLogger::log_end(uint cor_id){

			int ret = LOG_SUCC;

			TempLog *temp_log = temp_logs_[cor_id];
			temp_log->close();
			return 0;

			if(temp_log->mac_backups_.size() != 0){
#if TX_LOG_STYLE > 0
				char* req_buf = msg_buf_alloctors[cor_id].get_req_buf() + sizeof(rpc_header) + sizeof(uint64_t);
				DBLogger::RequestHeader* request_header = (DBLogger::RequestHeader*)req_buf;

				// DBLogger::RequestHeader* request_header =
				// 	(DBLogger::RequestHeader*)(temp_log->start_ - sizeof(DBLogger::RequestHeader));

				request_header->length = sizeof(uint64_t);
				memcpy(request_header->offsets, temp_log->remote_ack_offsets_, sizeof(uint64_t) * num_nodes_);

	            *((uint64_t*)(req_buf + sizeof(DBLogger::RequestHeader))) = *((uint64_t*)temp_log->start_);

				rpc_handler_->broadcast_to((char *)request_header,RPC_LOGGING_COMMIT,sizeof(uint64_t) + sizeof(DBLogger::RequestHeader),
										  cor_id,RRpc::REQ,temp_log->remote_mac_ids_,temp_log->remote_mac_num_);

#else

	            for(int i = 0; i < temp_log->remote_mac_num_; i++){
					int remote_id = temp_log->remote_mac_ids_[i];
					// Qp* qp = qp_vec_[remote_id];
					Qp* qp = get_qp(remote_id);
					Qp::IOStatus ret = qp->rc_post_send(IBV_WR_RDMA_WRITE,temp_log->start_,
						sizeof(uint64_t),temp_log->remote_ack_offsets_[remote_id],IBV_SEND_INLINE);
					assert(ret == Qp::IO_SUCC);
				}
				// for(int i = 0; i < temp_log->remote_mac_num_; i++){
				// 	int remote_id = temp_log->remote_mac_ids_[i];
				//   Qp* qp = qp_vec_[remote_id];
				//   if(qp->poll_completion() != Qp::IO_SUCC){
				//     assert(false);
				//     return LOG_TIMEOUT;
				//   }
				// }

#endif
			}
			temp_log->close();
		}

		int DBLogger::log_abort(uint cor_id){

			int ret = LOG_SUCC;
			// printf("log_abort : %u\n", cor_id);

			TempLog *temp_log = temp_logs_[cor_id];
			temp_log->close();

		}

#if TX_LOG_STYLE == 1
		void DBLogger::logging_handler(int id,int cid,char *msg,void *arg){

			DBLogger::RequestHeader* request_header = (DBLogger::RequestHeader*)msg;
			uint64_t offset = request_header->offsets[current_partition];
			// print_log_header(msg + sizeof(uint64_t) + sizeof(DBLogger::RequestHeader));

			memcpy(base_local_buf_ptr_ + offset, msg + sizeof(DBLogger::RequestHeader),
				request_header->length);

			char* reply_msg = rpc_handler_->get_reply_buf();
			DBLogger::ReplyHeader* reply_header = (DBLogger::ReplyHeader*)reply_msg;
			reply_header->ack = LOG_ACK_NUM;
			rpc_handler_->send_reply(reply_msg,sizeof(DBLogger::ReplyHeader),id,cid);
		}
#endif

#if TX_LOG_STYLE > 0
		void DBLogger::logging_commit_handler(int id,int cid,char *msg,void *arg){
			DBLogger::RequestHeader* request_header = (DBLogger::RequestHeader*)msg;
			uint64_t offset = request_header->offsets[current_partition];
			char* ptr = base_local_buf_ptr_ + offset;

			memcpy(ptr, msg + sizeof(DBLogger::RequestHeader),request_header->length);
			//#if USE_BACKUP_STORE && !USE_BACKUP_BENCH_WORKER
#if TX_BACKUP_STORE
			char* head_ptr = ptr - *((uint64_t*)ptr) - sizeof(uint64_t);
			uint64_t msg_size;
			char* tail_ptr = check_log_completion(head_ptr, &msg_size);
			assert(tail_ptr != NULL);
			clean_log(head_ptr,tail_ptr);
			memset((void*)head_ptr,0,msg_size + sizeof(uint64_t) + sizeof(uint64_t));
#endif
		}

#endif

		char* DBLogger::check_log_completion(volatile char* ptr, uint64_t *msg_size){
			uint64_t header_log_size = *((uint64_t*)ptr);
			// printf("header_log_size:%lx, offset:%lx\n",header_log_size, (uint64_t)(ptr - (uint64_t)cm_->conn_buf_));
			if(header_log_size >= THREAD_PADDING_SIZE)
				return NULL;
			uint64_t tailer_log_size = *((uint64_t*)(ptr + header_log_size + sizeof(uint64_t)));
			// printf("tailer_log_size:%lx, offset:%lx\n",tailer_log_size, (uint64_t)(ptr + header_log_size + sizeof(uint64_t) - (uint64_t)cm_->conn_buf_));
			if(header_log_size != tailer_log_size)
				return NULL;

			TXHeader* header = (TXHeader*)(ptr + sizeof(uint64_t));
			TXTailer* tailer = (TXTailer*)(ptr + sizeof(uint64_t) + header_log_size - sizeof(TXTailer));

			if(header->magic_num != LOG_HEADER_MAGIC_NUM ||
					tailer->magic_num != LOG_TAILER_MAGIC_NUM)
				return NULL;

			if(msg_size)*msg_size = header_log_size;
			return (char*)tailer;
		}

		void DBLogger::clean_log(char* log_ptr, char* tailer_ptr){

			TXHeader* header = (TXHeader*)(log_ptr + sizeof(uint64_t));
			TXTailer* tailer = (TXTailer*)(tailer_ptr);

			bool use_global_seq = header->global_seq;
			uint64_t global_seq = tailer->seq;

			char* ptr = log_ptr + sizeof(uint64_t) + sizeof(TXHeader);

			while(ptr + ROUND_UP_BASE <= tailer_ptr){
				EntryMeta* entry_meta = (EntryMeta*)ptr;
				int table_id = entry_meta->table_id;
				uint32_t size = entry_meta->size;
				uint64_t seq = use_global_seq ? global_seq : entry_meta->seq;
				uint64_t key = entry_meta->key;
				char* value_ptr = ptr + sizeof(EntryMeta);

				// printf("table_id:%d\n", table_id);
				// printf("key:%lu, 0x%lx\n", key, key);
				// printf("size:%d\n", size);
				// printf("seq:%lu\n", seq);
				// printf("value:%lu\n", *((uint64_t*)value_ptr));
				assert(log_cleaner_ != NULL);
				log_cleaner_->clean_log(table_id, key, seq, value_ptr, size);

				ptr += sizeof(EntryMeta) + size;

			}
		}

		void DBLogger::print_total_mem(){
			printf("base_ptr:%p\n", base_ptr_);
			for(int node_id = 0; node_id < num_nodes_; node_id++){
				print_node_area_mem(node_id);
			}
		}

		void DBLogger::print_node_area_mem(int node_id){
			for(int thread_id = 0 ; thread_id < nthreads; thread_id++){
				print_thread_area_mem(node_id, thread_id);
			}
		}

		void DBLogger::print_thread_area_mem(int node_id, int thread_id){
			printf("print thread area memory->node:%d, thread:%d\n",node_id,thread_id);
			char* meta_ptr = base_ptr_ + node_id * node_area_size_ + thread_id * THREAD_AREA_SIZE;
			char* buf_ptr = meta_ptr + THREAD_META_SIZE;
			printf("[\n");
			while(1){
				buf_ptr = print_log(buf_ptr);
				if(buf_ptr == NULL)break;
				// printf("buf:%p\n", buf_ptr);
			}
			printf("]\n");
		}

		char* DBLogger::print_log(char* ptr){
			char* tailer_ptr = check_log_completion(ptr);
			if(tailer_ptr == NULL)return NULL;
			printf("{\n");
			ptr += sizeof(uint64_t);
			ptr = print_log_header(ptr);
			// printf("tailer:%p\n", tailer_ptr);
			// printf("buf:%p\n", buf_ptr);
			printf("(\n");
			while(ptr + ROUND_UP_BASE <= tailer_ptr){
				ptr = print_log_entry(ptr);
				printf(",\n");
				// printf("buf:%p\n", buf_ptr);
			}
			printf(")\n");
			ptr = print_log_tailer(tailer_ptr);
			ptr += sizeof(uint64_t);
			printf("}\n");
			return ptr;
		}

		char* DBLogger::print_log_header(char *ptr){
			TXHeader* header = (TXHeader*)ptr;
			printf("magic_num:%lu\n", header->magic_num);
			printf("global_seq:%lu\n", header->global_seq);
			return ptr += sizeof(TXHeader);
		}

		char* DBLogger::print_log_entry(char *ptr){
			EntryMeta* entry_meta = (EntryMeta*)ptr;
			printf("table_id:%d\n", entry_meta->table_id);
			printf("key:%lu, 0x%lx\n", entry_meta->key, entry_meta->key);
			printf("size:%d\n", entry_meta->size);
			printf("seq:%lu\n", entry_meta->seq);
			ptr += sizeof(EntryMeta);
			if(entry_meta->size == sizeof(uint64_t)){
				printf("value:%lu\n", *((uint64_t*)ptr));
			}
			return ptr + entry_meta->size;
		}

		char* DBLogger::print_log_tailer(char *ptr){
			TXTailer* tailer = (TXTailer*)ptr;
			printf("magic_num:%lu\n", tailer->magic_num);
			printf("seq:%lu\n", tailer->seq);
			return ptr += sizeof(TXTailer);
		}
	}
}
