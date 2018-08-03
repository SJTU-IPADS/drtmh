#ifndef TESTCONFIG_XML
#define TESTCONFIG_XML

#include <vector>
#include <string>
#include <sstream>
#include <assert.h>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <boost/algorithm/string.hpp>

using namespace boost;
using boost::property_tree::ptree;
using namespace property_tree;

using namespace std;

class TestConfig{
 public:
	TestConfig(){ }
	~TestConfig(){ }

	void open_file(string configPath) {
		read_xml(configPath, pt_);
	}

	inline void read_op() {
		// xml version
		op_ = pt_.get<int>("param.op");
	}

	inline void read_req_length() {
		req_length_ = pt_.get<int>("param.reqLength");
	}

	inline void read_work_count() {
		work_count_ = pt_.get<int>("param.workCount");
	}

	inline void read_port() {
		port_ = pt_.get<int>("param.port");
	}

	inline void read_batch_size() {
		batch_size_ = pt_.get<int>("param.batchSize");
	}

	inline void read_iter_num() {
		iters_ = pt_.get<int>("param.iter");
	}

	inline void read_network() {
		BOOST_FOREACH(ptree::value_type &v,
					  pt_.get_child("param.network")) {
			string s = v.second.data();
			get_pure_string(s);
			network_.push_back(s);
		}
	}

	inline void read_user_bufsize() {
		string s_bufsize = pt_.get<string>("param.userBuf");
		get_pure_string(s_bufsize);
		user_bufsize_ = convert_buf_size(s_bufsize);
	}

	inline void read_rc_per_thread() {
		rc_per_thread_ = pt_.get<int>("param.rcPerThread");
	}

	inline void read_uc_per_thread() {
		uc_per_thread_ = pt_.get<int>("param.ucPerThread");
	}

	inline void read_ud_per_thread() {
		ud_per_thread_ = pt_.get<int>("param.udPerThread");
	}

	inline void read_enable_single_mr() {
		enable_single_mr_ = pt_.get<int>("param.enableSingleMR");
	}

	int op_;
	int req_length_;
	int work_count_;
	int port_;
	int batch_size_;
	int iters_;
	int rc_per_thread_;
	int uc_per_thread_;
	int ud_per_thread_;
	int enable_single_mr_;
	uint64_t user_bufsize_;
	vector<string> network_;

private:
	ptree pt_;
	uint64_t convert_buf_size(string buf_size) {

		uint64_t size;
		stringstream ss(buf_size);
		assert(ss >> size);
		char unit;
		if(!(ss >> unit)) { // pass;
		} else {
			switch(unit){
			case 'g': case 'G':
				size *= 1024;
			case 'm': case 'M':
				size *= 1024;
			case 'k': case 'K':
				size *= 1024;
			}
		}
		return size;
	}

	void get_pure_string(string &s) {
		boost::algorithm::trim_right(s);
		boost::algorithm::trim_left(s);
	}
};

#endif

