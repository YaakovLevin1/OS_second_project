cmake_minimum_required(VERSION 3.16)
project(OS_second_project)

set(CMAKE_CXX_STANDARD 20)

add_executable(OS_second_project
        tests2/test_1-1_thread_1_process_updated.cpp
        MapContext.cpp
        MapReduceJob.cpp
        ReduceContext.cpp
)

target_include_directories(OS_second_project PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})