#ifndef NOCC_TX_2PL
#define NOCC_TX_2PL

#include "tx_handler.h"
#include "db/remote_set.h"
#include "memstore/memdb.h"

#include <map>

#define META_LENGTH  16


using namespace nocc::db;

namespace nocc {
  namespace db {

    class DBLock : public TXHandler {

    public:


    }; // end class 2pl

  };
};

#endif
