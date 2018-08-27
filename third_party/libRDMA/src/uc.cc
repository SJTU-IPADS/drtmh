#include <arpa/inet.h> //used for checksum

#include "rdmaio.h"
#include "utils.h"
#include "helper_func.hpp"

#include "pre_connector.hpp"

namespace rdmaio {

extern int tcp_base_port; // tcp listening port
extern int node_id; // this instance's node id
extern std::vector<std::string> network; // topology
extern int num_uc_qps;


bool Qp::connect_uc() {
#if 0
    if(inited_) {
        return true;
    } else {
        //			fprintf(stdout,"qp %d %d not connected\n",tid,nid);
    }

    int remote_qid = _QP_ENCODE_ID(node_id,UC_ID_BASE + tid * num_uc_qps + idx_);

    char address[30];
    int address_len = snprintf(address,30,"tcp://%s:%d",network[nid].c_str(),tcp_base_port);
    assert(address_len < 30);

    QPConnArg arg;
    arg.qid = remote_qid;
    arg.sign = MAGIC_NUM;
    arg.calculate_checksum();

    auto socket = PreConnector::get_send_socket(network[nid],tcp_base_port);
	if(socket < 0) {
		// cannot establish the connection, shall retry
		return false;
	}

	auto n = PreConnector::send_to(socket,(char *)(&arg),sizeof(QPConnArg));
	if(n != sizeof(QPConnArg)) {
        close(socket);
        return false;
    }

	if(!PreConnector::wait_recv(socket)) {
        close(socket);
        return false;
    }

    int buf_size = sizeof(QPReplyHeader) + sizeof(RdmaQpAttr);
    char *reply_buf = new char[buf_size];

    n = recv(socket,reply_buf,buf_size, MSG_WAITALL);
	if(n != sizeof(RdmaQpAttr) + sizeof(QPReplyHeader)) {
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
        assert(false);
    }

    RdmaQpAttr qp_attr;
    memcpy(&qp_attr,(char *)reply_buf + sizeof(QPReplyHeader),sizeof(RdmaQpAttr));

    // verify the checksum
    uint64_t checksum = ip_checksum((void *)(&(qp_attr.buf)),sizeof(RdmaQpAttr) - sizeof(uint64_t));
    assert(checksum == qp_attr.checksum);

    change_qp_states(&qp_attr,port_idx);

    inited_ = true;

    delete reply_buf;
#endif
    return true;
}

Qp::IOStatus Qp::uc_post_send(ibv_wr_opcode op,char *local_buf,int len,uint64_t off,int flags) {

    IOStatus rc = IO_SUCC;
    struct ibv_send_wr sr, *bad_sr;
    struct ibv_sge sge;

    assert(this->qp->qp_type == IBV_QPT_UC);
    sr.opcode = op;
    sr.num_sge = 1;
    sr.next = NULL;
    sr.sg_list = &sge;

    sr.send_flags = flags;

    sge.addr = (uint64_t)local_buf;
    sge.length = len;
    sge.lkey = dev_->conn_buf_mr->lkey;

    sr.wr.rdma.remote_addr = remote_attr_.memory_attr_.buf + off;
    sr.wr.rdma.rkey = remote_attr_.memory_attr_.rkey;

    rc = (IOStatus)ibv_post_send(qp, &sr, &bad_sr);
    CE(rc, "ibv_post_send error\n");
    return rc;
}

Qp::IOStatus Qp::uc_post_doorbell(RdmaReq *reqs, int batch_size) {

    IOStatus rc = IO_SUCC;
    assert(batch_size <= MAX_DOORBELL_SIZE);

    struct ibv_send_wr sr[MAX_DOORBELL_SIZE], *bad_sr;
    struct ibv_sge sge[MAX_DOORBELL_SIZE];

    assert(this->qp->qp_type == IBV_QPT_UC);
    bool poll = false;
    for(uint i = 0; i < batch_size; i++) {
        // fill in the requests
        sr[i].opcode = reqs[i].opcode;
        sr[i].num_sge = 1;
        sr[i].next = (i == batch_size - 1) ? NULL : &sr[i + 1];
        sr[i].sg_list = &sge[i];
        sr[i].send_flags = reqs[i].flags;

        if(first_send()){
            sr[i].send_flags |= IBV_SEND_SIGNALED;
        }
        if(need_poll()){
            poll = true;
        }

        sge[i].addr = reqs[i].buf;
        sge[i].length = reqs[i].length;
        sge[i].lkey = dev_->conn_buf_mr->lkey;

        sr[i].wr.rdma.remote_addr =
				remote_attr_.memory_attr_.buf + reqs[i].wr.rdma.remote_offset;
        sr[i].wr.rdma.rkey = remote_attr_.memory_attr_.rkey;
    }
    if(poll) rc = poll_completion();
    rc = (IOStatus)ibv_post_send(qp, &sr[0], &bad_sr);
    CE(rc, "ibv_post_send error");
    return rc;
}


}
