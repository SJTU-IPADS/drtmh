/* below are internal structures used by the remote-set ****************/
struct RemoteReqObj {
  int tableid;
  uint64_t key;
  int pid;
  int size;
};

struct CommitHeader {
  int32_t  total_size;
  uint64_t commit_seq;
};

union KeyType {
  char long_key[40];
  uint64_t short_key;
};

struct RemoteSetItem {
  uint8_t pid;
  int8_t tableid;
  uint64_t seq;
  char *val;
  uint64_t key;
  MemNode *node;
};

struct RequestHeader {

  uint8_t  cor_id;
  // This payload is used for application sepecific metadata
  //int8_t   tx_id;
  int8_t   num;
  uint64_t padding;
  //char *ptr;

} __attribute__ ((aligned (8)));

struct RemoteSetRequestItem {
  REQ_TYPE type;
  uint8_t  pid;
  int8_t  tableid;
  uint16_t  idx;
  MemNode *node; /* read remote memnode for later locks */
  uint64_t seq;
#if LONG_KEY == 1
  KeyType  key;
#else
  uint64_t key;
#endif
  //      bool     is_write;
} __attribute__ ((aligned (8)));

struct RemoteLockItem {
  uint8_t pid;
  uint64_t seq;
  MemNode *node;
  uint8_t   tableid;
} __attribute__ ((aligned (8)));

struct RemoteWriteItem {
  uint8_t pid;
  MemNode *node;
  uint16_t payload;
} __attribute__ ((aligned (8)));

struct ReplyHeader {
  uint8_t  num_items_;
  uint8_t  partition_id_;
  /* payload shall include this header */
  /* also this payload can be used to carry data*/
  uint32_t payload_;
} __attribute__ ((aligned (8)));

struct RemoteSetReplyItem {
  uint8_t    pid;
  uint64_t   seq;
  MemNode*   node;
  uint16_t   payload;
  int8_t     tableid;
  uint8_t    idx;
} __attribute__ ((aligned (8)));

// single object request buffer
struct RequestItem {
  union {
    uint64_t seq;
    uint64_t key;
  };
  uint8_t  tableid;
  uint8_t  idx;
} __attribute__ ((aligned(8)));

struct RequestItemWrapper {
  struct RRpc::rrpc_header  rpc_padding;
  struct RequestItem req;
} __attribute__ ((aligned(8)));

struct ReplyItem {
  uint64_t seq;
  MemNode *node;
  uint8_t  idx;
};
