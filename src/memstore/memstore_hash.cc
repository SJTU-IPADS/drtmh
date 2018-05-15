#include "memstore/memstore_hash.h"

namespace leveldb {
	__thread MemstoreHashTable::HashNode *MemstoreHashTable::dummynode_ = NULL;
}
