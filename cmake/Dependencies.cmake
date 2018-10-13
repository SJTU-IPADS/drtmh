#
# Install dependencies
#

## install Rmalloc (built from ssmalloc)
include( ExternalProject )
set( SSMALLOC_INSTALL_DIR ${CMAKE_SOURCE_DIR}/third_party/libRDMA)
ExternalProject_Add( ralloc
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/third_party/libRDMA/ralloc
  CONFIGURE_COMMAND mkdir -p ${CMAKE_SOURCE_DIR}/third_party/libRDMA/lib
  BUILD_COMMAND make
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install
)
set( LIBSSMALLOC_HEADERS ${SSMALLOC_INSTALL_DIR} ${SSMALLOC_INSTALL_DIR}/include )
set( LIBSSMALLOC_LIBRARIES ${SSMALLOC_INSTALL_DIR}/lib )


## install Boost
set( BOOST_INSTALL_DIR ${CMAKE_SOURCE_DIR}/third_party/boost )
ExternalProject_Add( libboost1.61
#  URL $ENV{HOME}/download/boost_1_61_0.tar.bz2
  URL https://sourceforge.net/projects/boost/files/boost/1.61.0/boost_1_61_0.tar.bz2
  URL_HASH SHA256=a547bd06c2fd9a71ba1d169d9cf0339da7ebf4753849a8f7d6fdb8feee99b640
  CONFIGURE_COMMAND ./bootstrap.sh --prefix=${BOOST_INSTALL_DIR} --with-libraries=system,coroutine
  BUILD_COMMAND ./b2
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND ./b2 install
)
set( LIBBOOST_HEADERS ${BOOST_INSTALL_DIR}/include )
set( LIBBOOST_LIBRARIES ${BOOST_INSTALL_DIR}/lib )

#
# Configure dependencies: both built-in and external
#
find_library( LIBZMQ NAMES zmq HINTS $ENV{HOME}/local/zeromq/lib)
find_library( LIBIBVERBS NAMES ibverbs)
if( LINK_STATIC_LIB )
  add_library( ssmalloc STATIC IMPORTED )
  set_target_properties( ssmalloc PROPERTIES
    IMPORTED_LOCATION ${LIBSSMALLOC_LIBRARIES}/libssmalloc.a
    )
  add_library( boost_system STATIC IMPORTED )
  set_target_properties( boost_system PROPERTIES
    IMPORTED_LOCATION ${LIBBOOST_LIBRARIES}/libboost_system.a
    )
  add_library( boost_coroutine STATIC IMPORTED )
  set_target_properties( boost_coroutine PROPERTIES
    IMPORTED_LOCATION ${LIBBOOST_LIBRARIES}/libboost_coroutine.a
    )
  add_library( boost_chrono STATIC IMPORTED )
  set_target_properties( boost_chrono PROPERTIES
    IMPORTED_LOCATION ${LIBBOOST_LIBRARIES}/libboost_chrono.a
    )
  add_library( boost_thread STATIC IMPORTED )
  set_target_properties( boost_thread PROPERTIES
    IMPORTED_LOCATION ${LIBBOOST_LIBRARIES}/libboost_thread.a
    )
  add_library( boost_context STATIC IMPORTED )
  set_target_properties( boost_context PROPERTIES
    IMPORTED_LOCATION ${LIBBOOST_LIBRARIES}/libboost_context.a
    )
else()
  add_library( ssmalloc SHARED IMPORTED )
  set_target_properties( ssmalloc PROPERTIES
    IMPORTED_LOCATION ${LIBSSMALLOC_LIBRARIES}/libssmalloc.so
    )
  add_library( boost_system SHARED IMPORTED )
  set_target_properties( boost_system PROPERTIES
    IMPORTED_LOCATION ${LIBBOOST_LIBRARIES}/libboost_system.so
    )
  add_library( boost_coroutine SHARED IMPORTED )
  set_target_properties( boost_coroutine PROPERTIES
    IMPORTED_LOCATION ${LIBBOOST_LIBRARIES}/libboost_coroutine.so
    )
endif()

## CPUINFO, used for check RTM supported, from pytorch
if (NOT TARGET cpuinfo)
  if (NOT DEFINED CPUINFO_SOURCE_DIR)
    set(CPUINFO_SOURCE_DIR "${CMAKE_SOURCE_DIR}/../third_party/cpuinfo" CACHE STRING "cpuinfo source directory")
  endif()

  set(CPUINFO_BUILD_TOOLS OFF CACHE BOOL "")
  set(CPUINFO_BUILD_UNIT_TESTS OFF CACHE BOOL "")
  set(CPUINFO_BUILD_MOCK_TESTS OFF CACHE BOOL "")
  set(CPUINFO_BUILD_BENCHMARKS OFF CACHE BOOL "")
  set(CPUINFO_LIBRARY_TYPE "static" CACHE STRING "")
  if(MSVC)
    if (CAFFE2_USE_MSVC_STATIC_RUNTIME)
      set(CPUINFO_RUNTIME_TYPE "static" CACHE STRING "")
    else()
      set(CPUINFO_RUNTIME_TYPE "shared" CACHE STRING "")
    endif()
  endif()
  add_subdirectory(
    "${CPUINFO_SOURCE_DIR}"
    "${CPUINFO_SOURCE_DIR}/cpuinfo/build")
  set_property(TARGET cpuinfo PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

find_path(ZMQ_CPP NAMES zmq.hpp HINTS  $ENV{HOME}/local/zeromq/include/)
include_directories( BEFORE ${LIBSSMALLOC_HEADERS} )
include_directories( BEFORE ${LIBBOOST_HEADERS} )
include_directories( BEFORE ${ZMQ_CPP} )
