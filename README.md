This projects contains **ROCC** framework and **RTX** distributed transactional processing. 

**ROCC** framework is aimed to simplify building distributed applications using `RDMA`. 

**RTX** is a distributed transaction system atop of ROCC.

- We are working hard to make it easy to use.

A snippet of how to use rocc framework.

- For one-sided operations

```c++
// for one-sided RDMA reads, as an exmaple
int payload = 64; 
uint64_t remote_addr = 0;        // remote memory address(offset)
char *buffer = Rmalloc(payload); // allocate a local buffer to store the result
auto qp = cm->get_rc_qp(qp_id);  // the the qp handler for the target machine
qp->rc_post_send(IBV_WR_RDMA_READ,
                buffer,
		payload,
                remote_addr,
                IBV_SEND_SIGNALED,
                cor_id_); // the current execute app id
sched->add_pending(qp,cor_id_); // add the pending request to the scheduler
indirect_yield();   // yield to other coroutine
// when executed, the buffer got the results of remote memory
Rfree(buffer);      // can free the buffer after that
```

- For two-sided(messaging) operations

```c++
int payload = 64;
char *msg_buf = Rmalloc(payload);  // send buffer
char *reply_buf = malloc(payload); // reply buffer
int id = remote_id; // remote server's id
rpc_handler->prepare_multi_req(reply_buf,1,cor_id_);
rpc_handler->broadcast_to(msg_buf,
                       rpc_id, 
                       payload,
                       cor_id_,
                       RRpc::REQ,
                       &id, // the address of server list to send
                       1);  // number of servers to send
                       
indirect_yield();   // yield to other coroutine
// when executed, the buffer got the results of remote memory
Rfree(msg_buffer);      // can free the buffer after that
free(reply_buffer);     
```



------

**Dependencies:**

- CMake `>= version 3.0` (For compiling)
- libtool
- g++`>= 4.8` (For possible HTM support)
- Zmq `XX` (May be any version shall be fine, and current version ` >= 4.2.2` has been tested )
- Zmq C++ binding
- Boost `1.61.0` (Only tested) (will be automatically installed)
- [LibRDMA](http://ipads.se.sjtu.edu.cn:1312/Windybeing/rdma_lib)

------

**Build:**

- Get RDMA lib
  - git submodule init
  - git submodule update


- `cmake .`
- `make noccocc`

------

**Run:**

`cd scripts; ./run2.py config7.xml noccocc "-t 12 -c 10 -r 256" micro 16 ` , 

where `t` states for number of threads used, `c` states for number of coroutines used and `r` is left for workload. The final augrment(16) is the number of machine used. 

A template config file:`config_template.xml` is presented in the root directory.

A template host file(host.xml) is presented in the scripts, which states for machine's ip/hostname in the cluster.

------

**We will soon make a detailed description and better configuration tools for ROCC**.

***
