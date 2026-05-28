cmake_minimum_required(VERSION 3.16)
project(OS_second_project)

set(CMAKE_CXX_STANDARD 20)

add_executable(OS_second_project
        MapContext.cpp
        MapReduceJob.cpp
        ReduceContext.cpp
)