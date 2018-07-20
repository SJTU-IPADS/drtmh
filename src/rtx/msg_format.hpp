#pragma once

// This header contains common msg definiation used in RTX

namespace nocc {

namespace rtx {

enum req_type_t {
  RTX_REQ_READ = 0,
  RTX_REQ_READ_LOCK,
  RTX_REQ_INSERT
};

// header of the requests message
struct RTXRequestHeader {
  uint8_t  cor_id;
  int8_t   num;     // num TX items in the message
  uint64_t padding; // a padding is left for user-defined entries
} __attribute__ ((aligned (8)));

struct RTXReadItem {
  req_type_t type;
  uint8_t  pid;
  uint64_t key;
  uint8_t  tableid;
  uint16_t idx;
  uint16_t len;
  inline RTXReadItem(req_type_t type,uint8_t pid,uint64_t key,uint8_t tableid,uint16_t len,uint8_t idx)
      :type(type),pid(pid),key(key),tableid(tableid),len(len),idx(idx)
  {
  }
} __attribute__ ((aligned (8)));

// entries of the request message
struct RTXRemoteWriteItem {
  uint8_t pid;
  union {
    MemNode *node;
    uint64_t key;
  };
  uint16_t payload;
  uint8_t  tableid;
} __attribute__ ((aligned (8)));

struct RtxWriteItem {
  uint8_t  pid;
  uint8_t  tableid;
  uint64_t key;
  uint16_t len;
  RtxWriteItem(int pid,int tableid,uint64_t key,int len) :
      pid(pid),tableid(tableid),key(key),len(len) {

  }
} __attribute__ ((aligned (8)));


}; // namespace rtx
}; // namespace nocc
