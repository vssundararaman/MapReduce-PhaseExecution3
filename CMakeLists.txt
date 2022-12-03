cmake_minimum_required(VERSION 3.24)
project(MapReducePhase3Exec)

set(CMAKE_CXX_STANDARD 17)

add_executable(MapReducePhase3Exec main.cpp
        headers/FileProcessorBase.hpp
        headers/MapperBase.hpp
        headers/ShufflerBase.hpp
        headers/ReducerBase.hpp
        )

target_link_libraries(MapReducePhase3Exec ${CMAKE_DL_LIBS})
