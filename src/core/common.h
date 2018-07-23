#ifndef NOCC_COMMON_H
#define NOCC_COMMON_H

#define NOCC_DECLARE_typed_var(type, name)      \
  namespace nocc {                              \
    extern type FLAGS_##name;                   \
  } // namespace nocc


namespace nocc {

#define DISABLE_COPY_AND_ASSIGN(classname)          \
  private:                                          \
    classname(const classname&) = delete;           \
  classname& operator=(const classname&) = delete


#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

#define ALWAYS_INLINE __attribute__((always_inline))

};

#endif
