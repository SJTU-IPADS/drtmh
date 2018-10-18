Since this project uses new advanced hardware features like RDMA, we briefly describe the hardware setting which is suitable here. 

## RDMA-enabled network card

**ROCC** can run most Mellanox RNICs (We say most because we have not tested on all of them yet :) ). It has been tested on `ConnctX-3`, `ConnectX-4`,`ConnectX-5`, 
both VPI and ROCE. 

Yet, we strongly suggest not to use `ConnectX-3 RNIC`, because its one-sided primitive requires 
*very very careful setting and tuning* to achieve a reasonable performance on **ROCC**. 
Later generations of RNICs (Like `ConnectX-4`) works well on **ROCC**. 
This is because `ConnectX-3 RNIC` has limited hardware resources such as the cache on the NIC.


## CPU 

**DrTM+H** requires Intel's restricted transactional memory~(RTM) for its concurrently data stores. 
It can call illegal instructions fault on CPU without this support on some workloads. Yet, **ROCC** does not depends on RTM.
