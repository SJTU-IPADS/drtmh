cmake_minimum_required (VERSION 2.6)

set (test
    "rc_ping"
    "ralloc_test"
    "msg_test"
    "mr_test"
    "one_sided"
    "multi_post_read"
    "hybrid"
    "ud_msg"
    "dev"
)

foreach(exe ${test})
  project (${exe})	   
endforeach(exe)

# The version number.
set (RDMA_IO_VERSION 1)

## Usefull marcos
macro(use_cxx11)
	if (CMAKE_VERSION VERSION_LESS "3.1")
		if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            		set (CMAKE_CXX_FLAGS "--std=gnu++11 ${CMAKE_CXX_FLAGS}")
      		endif ()
  	else ()
      		set (CMAKE_CXX_STANDARD 11)
  	endif ()
endmacro(use_cxx11)

## add c++11 support
use_cxx11()
set (CMAKE_CXX_FLAGS " -pthread -g -O2 ${CMAKE_CXX_FLAGS}")	

# add the binary tree to the search path for include files
# so that we will find TutorialConfig.h

set ( PROJECT_ROOT_DIR "${PROJECT_BINARY_DIR}/.." )

include_directories("${PROJECT_ROOT_DIR}/")
include_directories("${PROJECT_ROOT_DIR}/include")
include_directories("${PROJECT_ROOT_DIR}/rdmatest")
include_directories("${PROJECT_ROOT_DIR}/libibverbs-1.2.1mlnx1/include")

file(GLOB SOURCES "${PROJECT_BINARY_DIR}/../src/*.cc" )

#foreach (exe ${test})
#add_executable(${exe} ${PROJECT_BINARY_DIR}/${exe}.cc ${SOURCES})
#endforeach(exe)

add_executable(rc_ping ${PROJECT_BINARY_DIR}/rc_ping_test/test.cc ${PROJECT_BINARY_DIR}/main.cc ${SOURCES})
add_executable(ud_msg ${PROJECT_BINARY_DIR}/ud_msg_test/test.cc ${PROJECT_BINARY_DIR}/main.cc ${SOURCES})
add_executable(ralloc_test ${PROJECT_BINARY_DIR}/ralloc_test.cc)
add_executable(msg_test ${PROJECT_BINARY_DIR}/ring_msg_test/test.cc ${PROJECT_BINARY_DIR}/main.cc ${SOURCES})
add_executable(mr_test  ${PROJECT_BINARY_DIR}/mem_test/test.cc  ${SOURCES})
add_executable(one_sided ${PROJECT_BINARY_DIR}/one_sided_test/test.cc ${SOURCES}  ${PROJECT_BINARY_DIR}/main.cc)
add_executable(multi_post_read ${PROJECT_BINARY_DIR}/multi_post_read_test/test.cc ${SOURCES}  ${PROJECT_BINARY_DIR}/main.cc)

add_executable(hybrid ${PROJECT_BINARY_DIR}/hybrid_test/test.cc ${SOURCES}  ${PROJECT_BINARY_DIR}/main.cc)
add_executable(dev    ${PROJECT_BINARY_DIR}/get_device.cc ${SOURCES})

## add libraries 
foreach (exe ${test})
target_link_libraries(${exe} zmq)
target_link_libraries(${exe} ibverbs)
target_link_libraries(${exe} numa)
target_link_libraries(${exe} ssmalloc)

install (TARGETS ${exe} DESTINATION  ${PROJECT_BINARY_DIR} )
endforeach(exe)
