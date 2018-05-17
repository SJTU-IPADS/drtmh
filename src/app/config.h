#ifndef APP_CONFIG_H_
#define APP_CONFIG_H_

// 1: naive RPC
// 2: naive + RPC batching
// 3: naive + RPC batching + speculative execuation
#define NAIVE 4       // does not batch RPC, send it one-by-one

#define READ_RANDOM   1   //

#endif
