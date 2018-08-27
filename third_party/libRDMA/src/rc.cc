#include <arpa/inet.h> //used for checksum

#include "rdmaio.h"
#include "utils.h"
#include "helper_func.hpp"

#include "pre_connector.hpp"

namespace rdmaio {

extern int tcp_base_port; // tcp listening port
extern int node_id; // this instance's node id
extern std::vector<std::string> network; // topology
extern int num_rc_qps;

bool Qp::connect_rc() {

	if(inited_) {
		return true;
	} else {
		//			fprintf(stdout,"qp %d %d not connected\n",tid,nid);
	}

	int remote_qid = _QP_ENCODE_ID(node_id,RC_ID_BASE + tid * num_rc_qps + idx_);

	char address[30];

	QPConnArg arg; memset((char *)(&arg),0,sizeof(QPConnArg));
	arg.qid = remote_qid;
	arg.sign = MAGIC_NUM;
    arg.tid  = tid;
    arg.nid  = nid;
	arg.calculate_checksum();
	//socket.send(request);

	// prepare socket to remote
	auto socket = PreConnector::get_send_socket(network[nid],tcp_base_port);
	if(socket < 0) {
		// cannot establish the connection, shall retry
		return false;
	}
	auto n = send(socket,(char *)(&arg),sizeof(QPConnArg),0);
	if(n != sizeof(QPConnArg)) {
		shutdown(socket,SHUT_RDWR);
        close(socket);
        return false;
    }

	// receive reply
	if(!PreConnector::wait_recv(socket)) {
		shutdown(socket,SHUT_RDWR);
		close(socket);
		return false;
	}

    int buf_size = sizeof(QPReplyHeader) + sizeof(RCQPAttr);
    char *reply_buf = new char[buf_size];

    n = recv(socket,reply_buf,buf_size, MSG_WAITALL);
	if(n != sizeof(RCQPAttr) + sizeof(QPReplyHeader)) {

		shutdown(socket,SHUT_RDWR);
        close(socket);
        delete reply_buf;
        usleep(1000);
        return false;
    }

	// close connection
	close(socket);

	QPReplyHeader *hdr = (QPReplyHeader *)(reply_buf);

	if(hdr->status == TCPSUCC) {

	} else if(hdr->status == TCPFAIL) {
		delete reply_buf;
		return false;
	} else {
		fprintf(stdout,"QP connect fail!, val %d\n",((char *)reply_buf)[0]);
		assert(false);
	}

	//RdmaQpAttr qp_attr;
	//memcpy(&qp_attr,(char *)reply_buf + sizeof(QPReplyHeader),sizeof(RdmaQpAttr));
	RCQPAttr qp_attr;
	memcpy(&qp_attr,(char *)reply_buf + sizeof(QPReplyHeader),sizeof(RCQPAttr));

	change_qp_states(&qp_attr,port_idx);

	inited_ = true;

	delete reply_buf;
	return true;
}

Qp::IOStatus
Qp::rc_post_send(ibv_wr_opcode op,char *local_buf,int len,uint64_t off,int flags,uint64_t wr_id,uint32_t imm) {

	IOStatus rc = IO_SUCC;
	struct ibv_send_wr sr, *bad_sr;
	struct ibv_sge sge;

	assert(this->qp->qp_type == IBV_QPT_RC);

	sge.addr = (uint64_t)local_buf;
	sge.length = len;

#ifdef PER_QP_PD
	sge.lkey = mr->lkey;
#else
	sge.lkey = dev_->conn_buf_mr->lkey;
#endif
	if(op == IBV_WR_RDMA_WRITE_WITH_IMM || IBV_WR_SEND_WITH_IMM){
		sr.imm_data = imm;
	}

	sr.wr_id = wr_id;

	sr.opcode = op;
	sr.num_sge = 1;
	sr.next = NULL;
	sr.sg_list = &sge;

	sr.send_flags = flags;

	sr.wr.rdma.remote_addr =
			remote_attr_.memory_attr_.buf + off;
	sr.wr.rdma.rkey = remote_attr_.memory_attr_.rkey;
	// printf("rkey:%lu\n", remote_attr_.rkey);

	rc = (IOStatus)ibv_post_send(qp, &sr, &bad_sr);
	return rc;
}

Qp::IOStatus Qp::rc_post_pending(ibv_wr_opcode op,
								 char *local_buf,int len,uint64_t off,int flags,uint64_t wr_id)
{
	int i = current_idx++;
	sr[i].opcode  = op;
	sr[i].num_sge = 1;
	sr[i].next    = &sr[i+1];
	sr[i].sg_list = &sge[i];
	sr[i].wr_id   = wr_id;
	sr[i].send_flags = flags;

	sge[i].addr   = (uintptr_t)local_buf;
	sge[i].length = len;
#ifdef PER_QP_PD
	sge[i].lkey = mr->lkey;
#else
	sge[i].lkey = dev_->conn_buf_mr->lkey;
#endif

	//if(need_poll()) poll_completion();

	sr[i].wr.rdma.remote_addr =
			remote_attr_.memory_attr_.buf + off;
	sr[i].wr.rdma.rkey = remote_attr_.memory_attr_.rkey;
	return IO_SUCC;
}

bool Qp::rc_flush_pending() {

	if(current_idx > 0) {
		sr[current_idx - 1].next    = NULL;
		sr[current_idx - 1].send_flags |= IBV_SEND_SIGNALED;
		ibv_post_send(qp, &sr[0], &bad_sr);
		current_idx = 0;
		return true;
	}
	return false;
}

Qp::IOStatus Qp::rc_post_doorbell(RdmaReq *reqs, int batch_size) {

	IOStatus rc = IO_SUCC;
	assert(batch_size <= MAX_DOORBELL_SIZE);
	assert(this->qp->qp_type == IBV_QPT_RC);

	bool poll = false;
	for(uint i = 0; i < batch_size; i++) {
		// fill in the requests
		sr[i].opcode = reqs[i].opcode;
		sr[i].num_sge = 1;
		sr[i].next = (i == batch_size - 1) ? NULL : &sr[i + 1];
		sr[i].sg_list = &sge[i];
		sr[i].send_flags = reqs[i].flags;
		sr[i].wr_id = reqs[i].rid;

		if(first_send()){
			sr[i].send_flags |= IBV_SEND_SIGNALED;
		}
		if(need_poll()){
			poll_completion();
		}

		sge[i].addr = reqs[i].buf;
		sge[i].length = reqs[i].length;
#ifdef PER_QP_PD
		sge[i].lkey = mr->lkey;
#else
		sge[i].lkey = dev_->conn_buf_mr->lkey;
#endif

		sr[i].wr.rdma.remote_addr =
				remote_attr_.memory_attr_.buf + reqs[i].wr.rdma.remote_offset;
		sr[i].wr.rdma.rkey = remote_attr_.memory_attr_.rkey;
	}
	rc = (IOStatus)ibv_post_send(qp, &sr[0], &bad_sr);
	CE(rc, "ibv_post_send doorbell error");
	return rc;
}

Qp::IOStatus Qp::rc_post_compare_and_swap(char *local_buf,uint64_t off,
										  uint64_t compare_value, uint64_t swap_value, int flags,uint64_t wr_id){

	IOStatus rc = IO_SUCC;
	struct ibv_send_wr sr, *bad_sr;
	struct ibv_sge sge;

	assert(this->qp->qp_type == IBV_QPT_RC);
	sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
	sr.num_sge = 1;
	sr.next = NULL;
	sr.sg_list = &sge;
	sr.send_flags = flags;
	sr.wr_id = wr_id;

	sge.addr = (uint64_t)local_buf;
	sge.length = sizeof(uint64_t);
#ifdef PER_QP_PD
	sge.lkey = mr->lkey;
#else
	sge.lkey = dev_->conn_buf_mr->lkey;
#endif

	sr.wr.atomic.remote_addr = remote_attr_.memory_attr_.buf + off;
	sr.wr.atomic.rkey = remote_attr_.memory_attr_.rkey;
	sr.wr.atomic.compare_add = compare_value;
	sr.wr.atomic.swap = swap_value;
	rc = (IOStatus)ibv_post_send(this->qp, &sr, &bad_sr);
	CE(rc, "ibv_post_send error");
	return rc;
}

Qp::IOStatus Qp::rc_post_fetch_and_add(char *local_buf,uint64_t off,
									   uint64_t add_value, int flags,uint64_t wr_id){

	IOStatus rc = IO_SUCC;
	struct ibv_send_wr sr, *bad_sr;
	struct ibv_sge sge;

	assert(this->qp->qp_type == IBV_QPT_RC);
	sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
	sr.num_sge = 1;
	sr.next = NULL;
	sr.sg_list = &sge;
	sr.send_flags = flags;
	sr.wr_id = wr_id;

	sge.addr = (uint64_t)local_buf;
	sge.length = sizeof(uint64_t);
#ifdef PER_QP_PD
	sge.lkey = mr->lkey;
#else
	sge.lkey = dev_->conn_buf_mr->lkey;
#endif

	sr.wr.atomic.remote_addr = remote_attr_.memory_attr_.buf + off;
	sr.wr.atomic.rkey = remote_attr_.memory_attr_.rkey;
	sr.wr.atomic.compare_add = add_value;
	rc = (IOStatus)ibv_post_send(this->qp, &sr, &bad_sr);
	CE(rc, "ibv_post_send error");
	return rc;
}

int RdmaCtrl::post_conn_recvs(int qid, int recv_num) {
	struct ibv_recv_wr *head_rr, *tail_rr, *temp_rr, *bad_rr = NULL;
	RdmaRecvHelper *recv_helper = recv_helpers_[qid];

	int rc = 0;
	int head = recv_helper->recv_head;
	int tail = head + recv_num - 1;
	if(tail >= recv_helper->max_recv_num) {
		tail -= recv_helper->max_recv_num;
	}

	head_rr = recv_helper->rr + head;//&recvWRs[head];
	tail_rr = recv_helper->rr + tail;//&recvWRs[tail];
	temp_rr = tail_rr->next;
	tail_rr->next = NULL;

	rc = ibv_post_recv(qps_[qid]->qp, head_rr, &bad_rr);
	CE(rc, "ibv_post_recvs error");
	tail_rr->next = temp_rr;

	/* Update recv head: go to the last wr posted and take 1 more step */
	recv_helper->recv_head = tail;
	MOD_ADD(recv_helper->recv_head, recv_helper->max_recv_num); /* 1 step */
	return rc;
}

int RdmaCtrl::poll_conn_recv_cqs(int qid){
	Qp *qp = qps_[qid];
	RdmaRecvHelper *recv_helper = recv_helpers_[qid];
	int poll_result, rc;
	struct ibv_wc* wc = recv_helper->wc;
	poll_result = ibv_poll_cq (qp->recv_cq, recv_helper->max_recv_num, wc);
	rc = poll_result;
	CE(poll_result < 0,"poll CQ failed\n");
	for(int i = 0; i < poll_result; i++){
		if (wc[i].status != IBV_WC_SUCCESS) {
			fprintf (stderr,
					 "got bad completion with status: 0x%x, vendor syndrome: 0x%x, with error %s\n",
					 wc[i].status, wc[i].vendor_err,ibv_wc_status_str(wc[i].status));
			rc = -1;
		}
		// fprintf(stdout,"poll Recv imm %d, buffer data: %d\n",ntohl(qp->recvWCs[i].imm_data),
		//   (*(uint32_t*)(qp->recvWCs[i].wr_id)));
	}
	recv_helper->idle_recv_num += poll_result;
	if(recv_helper->idle_recv_num > recv_helper->max_idle_recv_num){
		post_conn_recvs(qid, recv_helper->idle_recv_num);
		recv_helper->idle_recv_num = 0;
	}
	return rc;
}

}
