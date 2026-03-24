cmake_minimum_required(VERSION 3.5)
project(rdma)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
cmake_policy(SET CMP0069 NEW)

file(GLOB_RECURSE SOURCES src/*.cpp)
list(FILTER SOURCES EXCLUDE REGEX ".*_old\\.cpp$")
add_executable(rdma ${SOURCES})

target_include_directories(rdma PRIVATE ${CMAKE_SOURCE_DIR}/include)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS ">>> Using Clang compiler: enabling max optimizations")
    target_compile_options(rdma PRIVATE -march=native -O3 -ffast-math -fvectorize -fslp-vectorize)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT ipo_supported OUTPUT error)
    if(ipo_supported)
        set_property(TARGET rdma PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif()
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(RDMA REQUIRED libibverbs librdmacm)
target_include_directories(rdma PRIVATE ${RDMA_INCLUDE_DIRS})
target_link_libraries(rdma PRIVATE ${RDMA_LIBRARIES})