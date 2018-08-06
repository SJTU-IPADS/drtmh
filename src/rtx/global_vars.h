#ifndef GLOBAL_VARS_H_
#define GLOBAL_VARS_H_

#include "view.h"

namespace nocc {

namespace rtx {

/**
 * New meta data for each record
 */
struct RdmaValHeader {
  uint64_t lock;
  uint64_t seq;
};


extern SymmetricView *global_view;

} // namespace rtx
} // namespace nocc

#endif
