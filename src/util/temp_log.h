#ifndef UTIL_TEMP_LOGGER_H
#define UTIL_TEMP_LOGGER_H

#include "ralloc.h"

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

// #define TEMP_LOG_DEBUGGING

const uint ROUND_UP_BASE = 8;

class TempLog{
public:
	bool in_use_;

	// start of the memory of temp log
	char *mem_start_;
	char *start_, *current_, *end_;

	uint32_t alloced_size_;
	std::set<int> mac_backups_;	//
	std::set<int> partitions_;	//

	uint32_t reserved_size_, reply_buf_size_;
	int *remote_mac_ids_; 	// remote machines to be written
	char *remote_mac_reps_, *current_rep_ptr_;

	int pending_acks_num_;
	int remote_mac_num_; 		// number of remote machines
	uint64_t *remote_log_offsets_;
	uint64_t *remote_ack_offsets_;

	const int base_alloc_size_ = 512;
	TempLog(int num_nodes,uint32_t reply_buf_size = 0, uint32_t reserved_size = 0):
			in_use_(false),
			mem_start_(NULL),
			start_(NULL),
			current_(NULL),
			end_(NULL),
			reserved_size_(reserved_size),
			reply_buf_size_(reply_buf_size),
			alloced_size_(0){

		remote_mac_ids_ = new int[num_nodes];
		if(reply_buf_size_)
			remote_mac_reps_ = new char[reply_buf_size_];
		remote_log_offsets_ = new uint64_t[num_nodes];
		remote_ack_offsets_ = new uint64_t[num_nodes];
  		remote_mac_num_ = 0;
	}

	~TempLog(){
		delete [] remote_mac_ids_;
		delete [] remote_mac_reps_;
		delete [] remote_log_offsets_;
		delete [] remote_log_offsets_;
	}

	void open(){
		assert(in_use_ == false);
		alloced_size_ = base_alloc_size_;

		mem_start_ = (char*)Rmalloc(alloced_size_);
		assert(mem_start_ != NULL);
		assert(((uint64_t)mem_start_) % ROUND_UP_BASE == 0);

		if(reply_buf_size_){
			//memset(remote_mac_reps_,0,reply_buf_size_);
			current_rep_ptr_ = remote_mac_reps_;
		}

		start_ = mem_start_ + reserved_size_;
		end_ = current_ = start_ + sizeof(uint64_t);
		print_info();
		in_use_ = true;
	}

	void close(){
		assert(in_use_ = true);
		Rfree(mem_start_);
		mem_start_ = start_ = current_ = end_ = NULL;

		partitions_.clear();
		mac_backups_.clear();

		remote_mac_num_ = 0;
		in_use_ = false;
	}

	inline char* append_entry(int size){
		assert(current_ == end_);
		resize(size);
		end_ += size;
		return current_;
	}

	inline void close_entry(){
		current_ = end_;
		print_info();
	}

	void log_size_round_up(){
		assert(current_ == end_);
		uint64_t log_size = get_total_size();
		if(log_size % ROUND_UP_BASE != 0){
			uint64_t delta = ROUND_UP_BASE - log_size % ROUND_UP_BASE;
			if(unlikely(log_size + delta > alloced_size_)){
				resize(delta);
			}
			current_ = end_ += delta;
		}
		print_info();

	}

	void resize(int size){
		bool need_resize = false;
		while(get_mem_size() + size > alloced_size_){
			alloced_size_ *= 2;
			need_resize = true;
		}
		if(need_resize){
			char *new_start = (char*)Rmalloc(alloced_size_);
			uint64_t old_size = get_mem_size();
			memcpy(new_start, mem_start_, old_size);
			Rfree(mem_start_);
			mem_start_ = new_start;
			start_ = mem_start_ + reserved_size_;
			end_ = current_ = mem_start_ + old_size;
		}
	}

	inline char* get_current_ptr(){
		return current_;
	}

	inline uint64_t get_total_size(){
		return (uint64_t)(end_ - start_);
	}

	inline uint64_t get_log_size(){
		return (uint64_t)(end_ - start_ - sizeof(uint64_t));
	}

	inline uint64_t get_mem_size(){
		return (uint64_t)(end_ - mem_start_);
	}


	void print_info(){
#ifdef TEMP_LOG_DEBUGGING
		printf("templog: %p %p %p\n", start_, current_,end_);
		printf("total_size: %lu\n", get_total_size());
		printf("alloced_size_: %d\n", alloced_size_);
#endif
	}
};

#endif
