#include "rdmaio.h"
#include "all.h"
#include "TestConfig.h"
#include <getopt.h>
#include <stdint.h>

#define MAX_THREAD 32
#define BUF_SIZE 4096
#define CACHELINE_SIZE 64
#define MAX_POSTLIST 128

double tput[MAX_THREAD];
TestConfig config;
void *run_server(void *arg) {
	struct thread_config configs = *(struct thread_config *) arg;
	int thread_id = configs.id;	/* Global ID of this server thread */

	RdmaCtrl *rdma = bootstrap_ud_rdma(configs.node_id, config.network, config.port,
		thread_id, NULL, config.userBufSize, config.udPerThread);
	memset((void *) rdma->dgram_buf_, 0, config.userBufSize);

	long long rolling_iter = 0;
	int ud_qp_i = 0;
	struct timespec start, end;
	clock_gettime(CLOCK_REALTIME, &start);

	int offset = CACHELINE_SIZE;
	while(offset < config.reqLength) {
		offset += CACHELINE_SIZE;
	}
	assert(offset * config.batchSize <= config.userBufSize);
	RdmaReq reqs[MAX_DOORBELL_SIZE];
	for(int i = 0; i < config.batchSize; i++){
		reqs[i].flags = IBV_SEND_SIGNALED;
		reqs[i].buf = (uint64_t) (uintptr_t) &rdma->dgram_buf_[offset * i];
		reqs[i].length = config.reqLength;
	}
	int num_nodes = rdma->get_num_nodes();
	while(1) {
		if(rolling_iter >= config.iterationNum) {
			clock_gettime(CLOCK_REALTIME, &end);
			double seconds = (end.tv_sec - start.tv_sec) + 
				(double) (end.tv_nsec - start.tv_nsec) / 1000000000;
			tput[thread_id] = config.iterationNum / seconds;
			printf("main: Server %d: %.2f Mops,latency %.4f\n", thread_id, tput[thread_id],1000000/tput[thread_id]);
			if(thread_id == 0) {
				double total_tput = 0;
				for(int i = 0; i < configs.num_threads; i++) {
					total_tput += tput[i];
				}
				printf("---------------main: Total tput = %.2f Mops.\n", total_tput);
			}
			rolling_iter = 0;
			clock_gettime(CLOCK_REALTIME, &start);
		}

		for(int nid = 0; nid < num_nodes; nid++){
			if(nid == configs.node_id)continue;
			int local_qid = _QP_ENCODE_ID(configs.node_id,ud_qp_i+thread_id*config.udPerThread+UD_ID_BASE);
			for(int i = 0; i < config.batchSize; i++){
				reqs[i].wr.ud.nid = nid;
				reqs[i].wr.ud.remote_qid = _QP_ENCODE_ID(nid,thread_id*config.udPerThread+UD_ID_BASE);
			}
			// rdma->post_ud_doorbell(local_qid, config.batchSize, reqs);
			// rolling_iter+=config.batchSize;
			rdma->post_ud(local_qid,reqs);
			rdma->poll_cq(local_qid);
			rolling_iter++;
		}
		ud_qp_i++;
		if(ud_qp_i == config.udPerThread) {
			ud_qp_i = 0;
		}
	}
	return NULL;
}

void *run_client(void *arg) {
	struct thread_config configs = *(struct thread_config *) arg;
	int thread_id = configs.id;

	RdmaCtrl *rdma = bootstrap_ud_rdma(configs.node_id, config.network, config.port,
		thread_id, NULL, config.userBufSize, config.udPerThread);

	long long rolling_iter = 0;
	struct timespec start, end;
	clock_gettime(CLOCK_REALTIME, &start);

	int qid = _QP_ENCODE_ID(configs.node_id, thread_id*config.udPerThread+UD_ID_BASE);
	Qp *qp = rdma->qps_[qid];
		
	while(1) {
		if(rolling_iter >= M_2) {
			clock_gettime(CLOCK_REALTIME, &end);
			double seconds = (end.tv_sec - start.tv_sec) + 
				(double) (end.tv_nsec - start.tv_nsec) / 1000000000;
			printf("main: Client %d: %.2f Mops\n", thread_id, M_2 / seconds);
			rolling_iter = 0;
		
			clock_gettime(CLOCK_REALTIME, &start);
		}
		// qp->poll_recv_cq();
		// rdma->post_ud_recv(qp->qp,
			// (void *) rdma->dgram_buf_, config.userBufSize, rdma->rdma_device_->dgram_buf_mr->lkey);
		// rolling_iter++;
		rolling_iter += rdma->poll_ud_recv_cqs(qid);
	}

	return NULL;
}

int main(int argc, char *argv[]) {
	int num_threads = -1, is_client = -1, node_id = -1;

	static struct option opts[] = {
		{ "num-threads",    1, NULL, 't' },
		{ "node-id",     1, NULL, 'n' },
		{ 0 }
	};

	/* Parse and check arguments */
	while(1) {
		int c = getopt_long(argc, argv, "t:n:", opts, NULL);
		if(c == -1)break;
		switch (c) {
			case 't':
				num_threads = atoi(optarg);
				break;
			case 'n':
				node_id = atoi(optarg);
				break;
			default:
				printf("Invalid argument %d\n", c);
				assert(false);
		}
	}

	config.readFile("testframework.cfg");
	config.readNetwork();
	config.readPort();
	config.readReqLength();
	config.readBatchSize();
	config.readPerMac(node_id);
	config.readUserBufSize();
	config.readUdPerThread();
	config.readRecvUdPerThread();
	config.readIterationNum();
	
	is_client = node_id == 0 ? 0 : 1;

	assert(num_threads >= 1);
	assert(config.batchSize <= MAX_DOORBELL_SIZE);
	assert(1 <= config.batchSize);
	assert(node_id >= 0);
	assert(config.reqLength >= 0);

	for(int i = 0; i < MAX_THREAD; i++) {
		tput[i] = 0;
	}
	/* Launch a single server thread or multiple client threads */
	printf("main: Using %d threads\n", num_threads);
	struct thread_config *configs = (thread_config*)malloc(num_threads * sizeof(struct thread_config));
	pthread_t *threads = (pthread_t*) malloc(num_threads * sizeof(pthread_t));

	for(int i = 0; i < num_threads; i++) {
		
		if(is_client) {
			configs[i].node_id = node_id;
			configs[i].id = i;
			configs[i].num_threads = num_threads;
			pthread_create(&threads[i], NULL, run_client, &configs[i]);
		} else {
			configs[i].node_id = node_id;
			configs[i].id = i;
			configs[i].num_threads = num_threads;
			pthread_create(&threads[i], NULL, run_server, &configs[i]);
		}
	}

	for(int i = 0; i < num_threads; i++) {
		pthread_join(threads[i], NULL);
	}

	return 0;
}




