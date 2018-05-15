#ifndef NOCC_OLTP_LOG_CLEANER_H
#define NOCC_OLTP_LOG_CLEANER_H

#include "memstore/memdb.h"

#include "view_manager.h"
#include <map>

namespace nocc{

  namespace oltp{

    // the abstract class for cleaning the log entry
    // be called after the receive each log entries
    class LogCleaner {
    	public:

    	LogCleaner();
		// should be 0 on success,-1 on error
		virtual int clean_log(int table_id, uint64_t key, uint64_t seq, char *val,int length) = 0;
    
		void add_backup_store(MemDB *backup_store);

		std::map<uint64_t,MemDB*> backup_stores_;

    };
   

  } 
}



#endif