// compile config parameters

#ifndef NOCC_CONFIG_H_
#define NOCC_CONFIG_H_

#include "all.h"

#define QP_NUMS 5

// execuation related configuration
#define NO_ABORT 0           // does not retry if the TX is abort, and do the execution
#define OCC_RETRY            // OCC does not issue another round of checks
#define OCC_RO_CHECK 1       // whether do ro validation of OCC

#define PROFILE_RW_SET 0     // whether profile TX's read/write set
#define PROFILE_SERVER_NUM 0 // whether profile number of server accessed per TX


// end of global configuration
#endif
