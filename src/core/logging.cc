#include "logging.h"

#include <iostream>

using namespace std;

namespace nocc {

// control flags for color
#define R_BLACK 39
#define R_RED 31
#define R_GREEN 32
#define R_YELLOW 33
#define R_BLUE 34
#define R_MAGENTA 35
#define R_CYAN 36
#define R_WHITE 37

std::string StripBasename(const std::string &full_path) {
  const char kSeparator = '/';
  size_t pos = full_path.rfind(kSeparator);
  if (pos != std::string::npos) {
    return full_path.substr(pos + 1, std::string::npos);
  } else {
    return full_path;
  }
}

inline string EndcolorFlag() {
  char flag[7];
  snprintf(flag,7, "%c[0m", 0x1B);
  return string(flag);
}

const int RTX_DEBUG_LEVEL_COLOR[] = {R_BLACK,R_YELLOW,R_BLACK,R_GREEN,R_MAGENTA,R_RED,R_RED};

MessageLogger::MessageLogger(const char *file, int line, int level)
    :level_(level) {
  if(level_ < ROCC_LOG_LEVEL)
    return;
  stream_ << "[" << StripBasename(std::string(file)) << ":" << line << "] ";
}


MessageLogger::~MessageLogger() {
  if(level_ >= ROCC_LOG_LEVEL) {
    stream_ << "\n";
    std::cout << "\033[" << RTX_DEBUG_LEVEL_COLOR[std::min(level_,6)] << "m"
              << stream_.str() << EndcolorFlag();
    if(level_ >= LOG_FATAL)
      abort();
  }
}

} // namespace nocc
