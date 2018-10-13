## ROCC and DrTM+H

### Intro

**ROCC** provides fast and simple programming of distributed applications, especially atop of *RDMA*. ROCC has been integrated with most RDMA features, including a variety of optimizations.  ROCC's code is in `src/core`. 

**DrTM+H** is a fast distributed transactional system atop of ROCC.

Below are some documentations of **ROCC**, which describes the hardware setting and computation model of this project, while also includes some tutorials to demonstrate that it is very easy to write high performance RDMA applications upon on **ROCC**.

* [Recommended Hardware settings](docs/rnic.md)

A snippet of how to use rocc framework. Notice that ROCC is independent of DrTM+H.

- Issue RDMA read/write operations 

```c++
// for one-sided RDMA READ, as an exmaple
int payload = 64;                // the size of data to read
uint64_t remote_addr = 0;        // remote memory address(offset)
char *buffer = Rmalloc(payload); // allocate a local buffer to store the result
auto qp = cm->get_rc_qp(mac_id);  // the the connection of target machine
rdma_sched_->post_send(qp,cor_id_,IBV_WR_RDMA_READ,local_buf,size,remote_addr,
                           IBV_SEND_SIGNALED); // send the read request and add completion event to the scheduler
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

We use DrTM+H to evaluate a transactional system's performance using ROCC. 

**Dependencies:**

For build:
- CMake `>= version 3.0` (For compiling)
- libtool (for complie only)
- g++`>= 4.8`
- Boost `1.61.0` (will be automatically installed by the build tool chain, since we use a specific version of Boost)

For build & for run time
- Zmq and its C++ binding
- libibverbs 

------

**A sample of how to build DrTM+H:**

- `git clone --recursive https://github.com/SJTU-IPADS/drtmh`.
- `sudo apt-get install libzmq3-dev`
- `sudo apt-get install libtool-bin`
- `sudo apt-get install cmake` 
- `cmake -DUSE_RDMA=1              //run using RDMA; set it to be 0 if only use TCP for execution`

         `-DONE_SIDED_READ=1       // enable RDMA friendly data store`
         
         `-DROCC_RBUF_SIZE_M=13240 // total RDMA buffer registered~(Unit of M)`
         
         `-DRDMA_STORE_SIZE=5000   // total RDMA left for data store~(Unit of M)`
         
         `-DRDMA_CACHE=0           // whether use location cache for data store`
         
         `-DTX_LOG_STYLE=2         // RTX's log style. 1 uses RPC, 2 uses RDMA`

- `make noccocc`
------

**Prepare for running:**

Two files, `config.xml`, `hosts.xml` must be used to configure the running of DrTM+H.  `config.xml` provides benchmark specific configuration, while `hosts.xml` provides the topology of the cluster.

The samples of these two files are listed in `${PATH_TO_DrTMH}/scripts` .

***

### **Run in a cluster:**

We provide a script to help deploy and run in a	cluster	setting. Using the following command on	the first machine defined in the `hosts.xml`.

`cd ${PATH_TO_DrTMH}/scripts; ./run2.py config.xml noccocc "-t 24 -c 10 -r 100" tpcc 16 ` , 

where `t` states for number of threads used, `c` states for number of coroutines used and `r` is left for workload. `tpcc` states for the application used, here states for running the TPC-C workload. The final augrment(16) is the number of machine used, according to the hosts.xml mentioned above. 

------

**We will soon make a detailed description and better configuration tools for ROCC**.

***
