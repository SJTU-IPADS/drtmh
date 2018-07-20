#ifndef NOCC_UTIL_DEBUG_H_
#define NOCC_UTIL_DEBUG_H_

#include <stdio.h>
#include <stdarg.h>
#include <string>

extern int verbose;

namespace nocc {


namespace util {

const int MAX_PRINT_BUF = 1024;
// TODO: merge these two class
class Debugger {
 public:
  static void debug_fprintf(FILE *out,std::string fmt, ...) {
    if(!verbose)return;

    va_list args;
    char buf[MAX_PRINT_BUF];

    va_start(args,fmt);
    vsprintf(buf,fmt.c_str(),args);
    va_end(args);

    fprintf(out,"%s",buf);
  }

  // a nice progrss printer
  // credits: razzak@stackoverflow
  static inline void print_progress(double percentage, const char *header = NULL,FILE *out = stdout) {
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
}; // class Debugger

}
#define ASSERT_PRINT(exp,file,fmt,...) {if(unlikely(!(exp))) {util::Debugger::debug_fprintf(file,fmt, ##__VA_ARGS__); assert(false);} }
} // nocc


#endif
