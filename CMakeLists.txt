cmake_minimum_required(VERSION 3.0)

link_libraries(pthread)

set(CMAKE_CXX_FLAGS "-std=c++0x -g -O0")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
## platform dependency
if(APPLE)
add_compile_options(
        -pthread
        -DOS_MACOSX
        -DLEVELDB_PLATFORM_POSIX
        -DLEVELDB_ATOMIC_PRESENT
        -fPIC
        -msse4.2
        -DLEVELDB_PLATFROM_POSIX_SSE
)
else()
    add_compile_options(
            -std=c++0x
            -fno-builtin-memcmp
            -pthread
            -DOS_LINUX
            -DLEVELDB_PLATFORM_POSIX
            -DLEVELDB_ATOMIC_PRESENT
            -DLEVELDB_PLATFORM_POSIX_SSE
            -DSNAPPY
            -fPIC
            -msse4.2
            -ggdb
            -O0
    )
    link_libraries(snappy)
endif()


set(SOURCE_FILES
        db/builder.cc
        db/c.cc
        db/dbformat.cc
        db/db_impl.cc
        db/db_iter.cc
        db/dumpfile
        db/filename.cc
        db/log_reader.cc
        db/log_writer.cc
        db/memtable.cc
        db/repair.cc
        db/table_cache.cc
        db/version_edit.cc
        db/version_set.cc
        db/write_batch.cc
        table/block_builder.cc
        table/block.cc
        table/filter_block.cc
        table/format.cc
        table/iterator.cc
        table/merger.cc
        table/table_builder.cc
        table/table.cc
        table/two_level_iterator.cc
        util/arena.cc
        util/bloom.cc
        util/cache.cc
        util/coding.cc
        util/comparator.cc
        util/crc32c.cc
        util/env.cc
        util/env_posix.cc
        util/filter_policy.cc
        util/hash.cc
        util/histogram.cc
        util/logging.cc
        util/options.cc
        util/status.cc
        port/port_posix.cc
        port/port_posix_sse.cc
        )

set(TEST_SUPPORT_FILES
        util/testharness.cc
        util/testutil.cc
        )

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/include)

add_library(leveldb ${SOURCE_FILES})
add_library(leveldb_test ${TEST_SUPPORT_FILES})

add_subdirectory(db)
add_subdirectory(util)
add_subdirectory(table)

add_executable(test_leveldb test_leveldb.cc)
target_link_libraries(test_leveldb leveldb)