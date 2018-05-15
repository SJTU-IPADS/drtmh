#ifndef NOCC_OLTP_BENCH_BACKUP_H
#define NOCC_OLTP_BENCH_BACKUP_H

//#include "framework.h"

#include "all.h"
#include "db/db_logger.h"
#include "core/utils/thread.h"
#include "util/util.h"

#include "view_manager.h"
#include "log_cleaner.h"

#include <string>

using namespace std;
using namespace rdmaio;

extern size_t backup_nthreads;

namespace nocc {

	using namespace db;
	using namespace util;

	namespace oltp {

		const uint32_t LOG_THRSHOLD = THREAD_BUF_SIZE / 8;

		class BackupBenchWorker : public ndb_thread {
		public:
			BackupBenchWorker(unsigned worker_id);
			~BackupBenchWorker();
			void run();

			char* check_log_completion(volatile char* ptr, uint64_t &msg_size);

			void clean_log(char* log_ptr, char* tailer_ptr);

			char *base_ptr_;        // ptr to start of logger memory
			uint64_t node_area_size_;

			uint64_t **feedback_offsets_;
			uint64_t **head_offsets_;
			uint64_t **tail_offsets_;


			unsigned int start_tid_;
			unsigned int final_tid_;
			unsigned int thread_gap_;

			RdmaCtrl *cm_;
			unsigned int worker_id_;
			unsigned int thread_id_;


		private:
			bool initilized_;
			int num_nodes_;

			std::vector<Qp *> qp_vec_;

			void thread_local_init();
			void create_qps();
		};

	}

}


#endif
