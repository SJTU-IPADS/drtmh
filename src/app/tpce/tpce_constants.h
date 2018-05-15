#ifndef NOCC_OLTP_TPCE_CON_H
#define NOCC_OLTP_TPCE_CON_H

// This file defines the constants used by tpce benchmark
namespace nocc {
  namespace oltp {
    namespace tpce {

      const int accountPerPartition = 5000;
      const int caPerPartition      = accountPerPartition * 10;
      const int companyPerPartition = 500 * (accountPerPartition / 1000);
      const int brokerPerPartition  = accountPerPartition / 100;
    };
  };
};

#endif
