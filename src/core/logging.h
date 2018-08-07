#pragma once

#include "common.h"

#include <cstring>
#include <sstream>
#include <string>
#include <cstdarg>


namespace nocc {

/**
 * \def LOG_FATAL
 *   Used for fatal and probably irrecoverable conditions
 * \def LOG_ERROR
 *   Used for errors which are recoverable within the scope of the function
 * \def LOG_WARNING
 *   Logs interesting conditions which are probably not fatal
 * \def LOG_EMPH
 *   Outputs as LOG_INFO, but in LOG_WARNING colors. Useful for
 *   outputting information you want to emphasize.
 * \def LOG_INFO
 *   Used for providing general useful information
 * \def LOG_DEBUG
 *   Debugging purposes only
 * \def LOG_EVERYTHING
 *   Log everything
 */

#define LOG_NONE 7
#define LOG_FATAL 6
#define LOG_ERROR 5
#define LOG_WARNING 4
#define LOG_EMPH 3
#define LOG_INFO 2
#define LOG_DEBUG 1
#define LOG_EVERYTHING 0

#ifndef RPCC_LOG_LEVEL
#define ROCC_LOG_LEVEL LOG_INFO
#endif

// logging macro definiations
// default log
#define LOG(n) \
    if (n >= ROCC_LOG_LEVEL)  \
        ::nocc::MessageLogger((char*)__FILE__, __LINE__, n).stream()

// log with tag
#define TLOG(n,t) \
    if(n >= ROCC_LOG_LEVEL) \
        ::nocc::MessageLogger((char*)__FILE__, __LINE__, n).stream() \
                  << "[" << (t) << "]"

#define LOG_IF(n,condition) \
    if(n >= ROCC_LOG_LEVEL && (condition))                              \
        ::nocc::MessageLogger((char*)__FILE__, __LINE__, n).stream()

#define ASSERT(condition) \
    if(unlikely(!(condition))) \
        ::nocc::MessageLogger((char*)__FILE__, __LINE__, LOG_FATAL + 1).stream()

#define VERIFY(n,condition) LOG_IF(n,(!(condition)))

// a nice progrss printer
// credits: razzak@stackoverflow
inline ALWAYS_INLINE
void PrintProgress(double percentage, const char *header = NULL,FILE *out = stdout) {

#define PBSTR "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
#define PBWIDTH 60
    int val = (int) (percentage * 100);
    int lpad = (int) (percentage * PBWIDTH);
    int rpad = PBWIDTH - lpad;

    const char *head_padding = header != NULL? header:"";

    fprintf(out,"\r%s %3d%% [%.*s%*s]", head_padding,val, lpad, PBSTR, rpad, "");
    if(percentage == 100)
        printf("\n");
}

class MessageLogger {
  public:
    MessageLogger(const char *file, int line, int level);
    ~MessageLogger();

    // Return the stream associated with the logger object.
    std::stringstream &stream() { return stream_; }
  private:
    std::stringstream stream_;
    int level_;
};

inline void MakeStringInternal(std::stringstream& /*ss*/) {}

template <typename T>
inline void MakeStringInternal(std::stringstream& ss, const T& t) {
    ss << t;
}

template <typename T, typename... Args>
inline void
MakeStringInternal(std::stringstream& ss, const T& t, const Args&... args) {
    MakeStringInternal(ss, t);
    MakeStringInternal(ss, args...);
}

template <typename... Args>
inline std::string MakeString(const Args&... args) {
    std::stringstream ss;
    MakeStringInternal(ss, args...);
    return std::string(ss.str());
}

// Specializations for already-a-string types.
template <>
inline std::string MakeString(const std::string& str) {
    return str;
}
inline std::string MakeString(const char* c_str) {
    return std::string(c_str);
}


} // namespace nocc
