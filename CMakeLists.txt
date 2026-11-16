cmake_minimum_required(VERSION 3.21)
project(CXXMessageQueue VERSION 0.6.0 LANGUAGES CXX)

include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(CXXMessageQueue INTERFACE)
add_library(mpsc::MessageQueue ALIAS CXXMessageQueue)
set_target_properties(CXXMessageQueue PROPERTIES EXPORT_NAME MessageQueue)

target_compile_features(CXXMessageQueue INTERFACE cxx_std_20)

target_include_directories(CXXMessageQueue INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Sources/CXXMessageQueue/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

install(TARGETS CXXMessageQueue EXPORT CXXMessageQueueTargets)

install(DIRECTORY Sources/CXXMessageQueue/include/mpsc DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT CXXMessageQueueTargets
    FILE CXXMessageQueueTargets.cmake
    NAMESPACE mpsc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CXXMessageQueue
)

option(CXXMessageQueue_BUILD_TESTING "Build the CXXMessageQueue tests" ${PROJECT_IS_TOP_LEVEL})

if(CXXMessageQueue_BUILD_TESTING)
    find_package(GTest REQUIRED)

    enable_testing()

    add_executable(run_tests test/message_queue_test.cpp)

    target_link_libraries(run_tests
        PRIVATE
            mpsc::MessageQueue
            GTest::gtest_main
    )

    if(MSVC)
        target_compile_options(run_tests PRIVATE /W4)
    else()
        target_compile_options(run_tests PRIVATE -Wall -Wextra)
    endif()

    include(GoogleTest)
    gtest_discover_tests(run_tests)
endif()

option(CXXMessageQueue_BUILD_BENCHMARK "Build the CXXMessageQueue benchmark" off)

if(CXXMessageQueue_BUILD_BENCHMARK)
    find_package(benchmark REQUIRED)

    add_executable(benchmark test/message_queue_benchmark.cpp)

    target_link_libraries(benchmark
        PRIVATE
            mpsc::MessageQueue
            benchmark::benchmark
    )

    if(MSVC)
        target_compile_options(benchmark PRIVATE /W4)
    else()
        target_compile_options(benchmark PRIVATE -Wall -Wextra)
    endif()
endif()
