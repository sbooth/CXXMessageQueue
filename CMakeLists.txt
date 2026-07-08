cmake_minimum_required(VERSION 3.15)
project(CXXMessageQueue VERSION 0.6.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(CXXMessageQueue
    Sources/CXXMessageQueue/mpsc/MessageQueue.cpp
    Sources/CXXMessageQueue/include/mpsc/MessageQueue.hpp
)
add_library(mpsc::MessageQueue ALIAS CXXMessageQueue)

target_include_directories(CXXMessageQueue PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Sources/CXXMessageQueue/include>
    $<INSTALL_INTERFACE:include>
)

if(MSVC)
    target_compile_options(CXXMessageQueue PRIVATE /W4)
else()
    target_compile_options(CXXMessageQueue PRIVATE -Wall -Wextra)
endif()

include(GNUInstallDirs)

install(TARGETS CXXMessageQueue
    EXPORT CXXMessageQueueTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY Sources/CXXMessageQueue/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT CXXMessageQueueTargets
    FILE CXXMessageQueueTargets.cmake
    NAMESPACE mpsc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CXXMessageQueue
)

option(BUILD_TESTING "Build tests" ON)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    enable_testing()

    add_executable(run_tests test/message_queue_test.cpp)

    target_link_libraries(run_tests
        PRIVATE
            mpsc::MessageQueue
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(run_tests)
endif()

option(BUILD_BENCHMARK "Build benchmark" OFF)

if(BUILD_BENCHMARK)
    find_package(benchmark REQUIRED)

    add_executable(benchmark test/message_queue_benchmark.cpp)

    target_link_libraries(benchmark
        PRIVATE
            mpsc::MessageQueue
            benchmark::benchmark
    )
endif()
