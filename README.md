## ROCC and RTX



### Intro

**ROCC** provides fast and simple programming of distributed applications, especially atop of *RDMA*. ROCC has been integrated with most RDMA features, including a variety of optimizations.  ROCC's code is in `src/core`. 

**RTX** is a fast distributed transactional system atop of ROCC.



A snippet of how to use rocc framework. Notice that ROCC is independent of RTX.

- Issue RDMA read/write operations 

```c++
// for one-sided RDMA READ, as an exmaple
int payload = 64;                // the size of data to read
uint64_t remote_addr = 0;        // remote memory address(offset)
char *buffer = Rmalloc(payload); // allocate a local buffer to store the result
auto qp = cm->get_rc_qp(mac_id);  // the the connection of target machine
qp->rc_post_send(IBV_WR_RDMA_READ, // READ request
                buffer,payload,
                remote_addr,
                IBV_SEND_SIGNALED,  // let NIC generate completion event
                cor_id_); // the current execute app id
sched->add_pending(qp,cor_id_); // add the pending request to the scheduler
indirect_yield();   // yield, not waiting the completion of READ
// when executed, the buffer got the results of remote memory
Rfree(buffer);      // can free the buffer after that
```

- For messaging operations. Notice that the message primitive has been optimized using RDMA.  ROCC provides 3 messaging primitive's implementations: 
  - Messaging with RDMA UD send/recv
  - Messaging with RDMA RC write-with-imm
  - Messaging with TCP/IP

```c++
int payload = 64;
char *msg_buf = Rmalloc(payload);  // send buffer
char *reply_buf = Rmalloc(payload); // reply buffer
int id = remote_id; // remote server's id
rpc_handler->prepare_multi_req(reply_buf,1,cor_id_); // tell RPC handler to receive 1 RPC replies
rpc_handler->broadcast_to(msg_buf, // request buf
                       rpc_id,     // remote function id to call
                       payload,    // size of the request
                       cor_id_,    // app coroutine id
                       RRpc::REQ,  // this is a request
                       &id, // the address of servers list to send
                       1);  // number of servers to send
                       
indirect_yield();   // yield to other coroutine
// when executed, the buffer got the results of remote memory
Rfree(msg_buffer);      // can free the buffer after that
Rfree(reply_buffer);     
```



------

### Build

We use RTX to test a transactional system's performance using ROCC. 

**Dependencies:**

- CMake `>= version 3.0` (For compiling)
- libtool
- g++`>= 4.8`
- Zmq
- Zmq C++ binding
- Boost `1.61.0` (Only tested) (will be automatically installed)
- Libibverbs 
- [LibRDMA](http://ipads.se.sjtu.edu.cn:1312/Windybeing/rdma_lib) 

------

**Build RTX:**

- git clone --recursive https://github.com/roccframework/rocc.git

- `cmake .`
- `make noccocc`

------

**Prepare:**

Two files, `config_template.xml`, `hosts.xml` must be used to configure the running of RTX.  `config_template.xml` provides benchmark specific configuration, while `hosts.xml` provides the topology of the cluster.

The samples of these two files are listed in ./scripts .

***

### **Run:**

`cd scripts; ./run2.py config_template.xml noccocc "-t 24 -c 10 -r 256" micro 16 ` , 

where `t` states for number of threads used, `c` states for number of coroutines used and `r` is left for workload. `micro` states for the application used, here states for running the micro bencharm. The final augrment(16) is the number of machine used, according to the hosts.xml mentioned above. 



------

**We will soon make a detailed description and better configuration tools for ROCC**.

***
