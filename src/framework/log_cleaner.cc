#include "log_cleaner.h"
#include "core/nocc_worker.h"


/* global config constants */
extern size_t nthreads;
extern size_t scale_factor;
extern size_t current_partition;
extern size_t total_partition;

namespace nocc{

	namespace oltp{

		extern View* my_view; // replication setting of the data

		LogCleaner::LogCleaner() {}
		
		void LogCleaner::add_backup_store(MemDB *backup_store){
			int num_backups, backups[MAX_BACKUP_NUM];
			num_backups = my_view->is_backup(current_partition, backups);
			assert(num_backups <= MAX_BACKUP_NUM);
			for(int i = 0; i < num_backups; i++){
				auto iter = backup_stores_.find(backups[i]);
				if(iter == backup_stores_.end()){
					printf("LogCleaner: %d backup_store[%d]  added\n", i,backups[i]);
					backup_stores_.insert(std::pair<uint64_t,MemDB*>(backups[i], backup_store));
					break;
				}
			}
		}
	}
}
