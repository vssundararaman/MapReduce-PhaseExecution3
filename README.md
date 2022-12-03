# Introduction

This repository relies on the [MapReduceLib](https://github.com/vssundararaman/MapReduce-PhaseLib3)

From the previous repo., we will copy the library files (* .so) and the interface files (*.hpp). The interface files
will reflect the abstract class implementations.

## Project structure

This repo. has been broken into sub-folders.

### headers

This folder will host all the base class implementations

### libs

This folder will host all the library files associated with each class.

#### fp

This folder will host all the library files associated with FileProcessor implementations.
Please refer to previous repo. for further details.

#### map

This folder will host all the library files associated with Mapper implementations.
Please refer to previous repo. for further details.

#### shuffle

This folder will host all the library files associated with Shuffler implementations.
Please refer to previous repo. for further details.

#### reduce

This folder will host all the library files associated with Reducer implementations.
Please refer to previous repo. for further details.

### main.cpp

This is the driver code that will execute MapReduce operations against a directory.
It has the following components -

1) runOrchestration
    * This function acts as overall orchestration mechanism
    * It checks for arguments and valid paths
    * It will kick off mapReduceWorkflow function
2) fileDirectoryChecks
    * Helper function that will check if all the files have been processed
    * Creates a SUCCESS.ind if checks pass
3) createLibHandle
    * This function will create a null pointer against the library file
4) createLibFunc
    * This is template function that will be used to create different factory functions associated with required class instances.
    * It uses the dlopen API to create the necessary explicit linkage
5) fileProcessInputs
    * This function is used to run futures operation for a single FileProcessorInput object
6) mapperOps
    * This function is used to run futures operation for a single mapper object
7) fileProcessMapOutputs
    * This function is used to run futures operation for a single FileProcessorMapOutput object
8) shufflerOps
    * This function is used to run futures operation for a single shuffler object
9) fileProcessShufOutputs
    * This function is used to run futures operation for a single FileProcessorShufOutput object
10) reducerOps
    * This function is used to run futures operation for a single Reducer object
11) fileProcessRedOutputs
    * This function is used to run futures operation for a single FileProcessorRedOutput object
12) mapReduceWorkflow
    * This is the entire Map Reduce pipeline
    * It relies on createLibFunc to create factory functions which are internally used to create class instances
    * It follows this workflow -
        * Load all data in a directory in a memory object
          * Each file has its own FileProcessorInput object result in a intermediate memory object broken into partitions
        * Tokenize data as part of Mapper operations
          * Each file's partition has its own Mapper object
        * Load mapped data to disk
          * Every Mapper object's output is written individually to disk
        * Read mapped data from disk to shuffler and perform shuffle operations
          * Each sub-folder is treated as an input to Shuffler object
          * If there are X folders in temp_mapper folder -> X Shuffler objects
        * Write shuffled data to disk
          * Each Shuffler object shuffles (aggregates) intermediate data - resulting in overall reduced file size
          * If there are X folders in temp_mapper folder and Y files within -> X folders and Y files within temp_shuffler folder
        * Read shuffled data from disk to reduce and perform reduce operations
          * Each sub-folder is treated as an input to Reducer object
          * If there are X folders in temp_shuffler folder -> X Reducer objects
        * Write reduced data to disk
          * Within a sub-folder, all individual shuffler files are reduced to a single file -> sent to final_output folder

The code base uses the following concurrency components 

    * std::futures
        * std::future<T>::get
    * std::async
    * std::mutex

    
### Building

To build the project, run the following command

    g++ -std=c++17 -o MRExec main.cpp headers/FileProcessorBase.hpp headers/MapperBase.hpp headers/ReducerBase.hpp headers/ShufflerBase.hpp -ldl

To execute 

    ./MRExec /home/ubuntu/CLionProjects/shakespeare_2 > logs/run_`date +'%Y%m%d%H%M%S'`.log
