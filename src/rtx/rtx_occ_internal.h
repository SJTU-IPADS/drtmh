// class RtxOCC
protected:
// a simple ReadSetItem for buffering read/write records
struct ReadSetItem {
  uint8_t  tableid;
  uint8_t  len;
  uint64_t key;
  union {
    MemNode *node;
    uint64_t off;
  };
  char    *data_ptr;
  uint64_t seq; // buffered seq
  uint8_t  pid;

  inline ReadSetItem(int tableid,uint64_t key,MemNode *node,char *data_ptr,uint64_t seq,int len,int pid):
      tableid(tableid),
      key(key),
      node(node),
      data_ptr(data_ptr),
      seq(seq),
      len(len),
      pid(pid)
  {
  }

  inline ReadSetItem(const ReadSetItem &item) :
      tableid(item.tableid),
      key(item.key),
      node(item.node),
      data_ptr(item.data_ptr),
      seq(item.seq),
      len(item.len),
      pid(item.pid)
  {
  }

}  __attribute__ ((aligned (8)));

struct RtxLockItem {
  uint8_t pid;
  uint8_t tableid;
  uint64_t key;
  uint64_t seq;

  RtxLockItem(uint8_t pid,uint8_t tableid,uint64_t key,uint64_t seq)
      :pid(pid),tableid(tableid),key(key),seq(seq)
  {
  }
} __attribute__ ((aligned (8)));

struct CommitItem {
  uint32_t len;
  uint32_t tableid;
  uint64_t key;
} __attribute__ ((aligned (8)));

struct ReadItem {
  uint32_t pid;
  uint32_t tableid;
  uint64_t key;
} __attribute__ ((aligned (8)));


struct ReplyHeader {
  uint16_t num;
};

struct OCCResponse {
  uint16_t payload;
  uint16_t idx;
  uint64_t seq;
};


//
/* 16 bit mac | 6 bit thread | 10 bit cor_id  */
#define ENCODE_LOCK_CONTENT(mac,tid,cor_id) ( ((mac) << 16) | ((tid) << 10) | (cor_id) )
#define DECODE_LOCK_MAC(lock) ( (lock) >> 16)
#define DECODE_LOCK_TID(lock) (((lock) & 0xffff ) >> 10)
#define DECODE_LOCK_CID(lock) ( (lock) & 0x3f)


#define LOCK_SUCCESS_MAGIC 73
#define LOCK_FAIL_MAGIC 12
