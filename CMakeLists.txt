# Copyright 2018 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cmake build file for C++ helloworld example.
# Assumes protobuf and gRPC have been installed using cmake.
# See cmake_externalproject/CMakeLists.txt for all-in-one cmake build
# that automatically builds all the dependencies before building helloworld.

cmake_minimum_required(VERSION 3.5.1)

project(loadbalancer C CXX)

if (NOT DEFINED CMAKE_CXX_STANDARD)
    set (CMAKE_CXX_STANDARD 14)
endif()

set(CMAKE_DEBUG_POSTFIX -dbg)
# uncomment to put in debug symbols or do on command line with:
#set(CMAKE_BUILD_TYPE Debug)
#add_compile_options(-Wall)
add_compile_options(-O3)


# Set architecture-related string for future installations of differing architectures
# set(ARCH ${CMAKE_HOST_SYSTEM_NAME}-${CMAKE_HOST_SYSTEM_PROCESSOR})
# message(STATUS "ARCH = " ${ARCH})

find_package(Threads REQUIRED)


# This branch assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
message(STATUS "_PROTOBUF_PROTOC = ${_PROTOBUF_PROTOC}")

if (NOT DEFINED ENV{GRPC_INSTALL_DIR})
    message(FATAL_ERROR "Define GRPC_INSTALL_DIR so gRPC libs and includes can be found")
endif()
set(GRPC_INSTALL_DIR $ENV{GRPC_INSTALL_DIR})
message(STATUS "set GRPC_INSTALL_DIR = ${GRPC_INSTALL_DIR}")

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC REQUIRED CONFIG PATHS ${GRPC_INSTALL_DR}/lib/cmake/grpc)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_REFLECTION gRPC::grpc++_reflection)
set(_GRPC_GRPCPP gRPC::grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)


# Installation directory may be defined on command line with -DINSTALL_DIR=<...>
if (NOT DEFINED INSTALL_DIR)
    set(INSTALL_DIR ${GRPC_INSTALL_DIR})
endif()
set(GRPC_INCLUDE_DIRS ${GRPC_INSTALL_DIR}/include)

message(STATUS "Installing into " ${INSTALL_DIR})
set(INSTALL_DIR_DEFINED 1)
set(CMAKE_INSTALL_PREFIX ${INSTALL_DIR})
message(STATUS "CMAKE_INSTALL_PREFIX reset to " ${CMAKE_INSTALL_PREFIX})


# Proto file
get_filename_component(hw_proto  "loadbalancer.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/loadbalancer.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/loadbalancer.pb.h")
set(hw_grpc_srcs  "${CMAKE_CURRENT_BINARY_DIR}/loadbalancer.grpc.pb.cc")
set(hw_grpc_hdrs  "${CMAKE_CURRENT_BINARY_DIR}/loadbalancer.grpc.pb.h")


add_custom_command(
        OUTPUT "${hw_proto_srcs}"  "${hw_proto_hdrs}"  "${hw_grpc_srcs}"  "${hw_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
        DEPENDS "${hw_proto}"
)

set(HEADER_FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/ejfat_packetize.hpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/ejfat_assemble_ersap.hpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/lb_cplane.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/lb_cplane.h"
        ${hw_proto_hdrs} ${hw_grpc_hdrs}
        )


message(STATUS "GRPC_INSTALL_DIR = $ENV{GRPC_INSTALL_DIR}")


# Include generated *.pb.h files
#include_directories("${CMAKE_CURRENT_BINARY_DIR}" "${GRPC_INSTALL_DIR}/include")
include_directories("${CMAKE_CURRENT_BINARY_DIR}")


add_library(ejfat_grpc SHARED
        lb_cplane.cc
        ${hw_grpc_srcs}
        ${hw_grpc_hdrs}
        ${hw_proto_srcs}
        ${hw_proto_hdrs}
        )

target_link_libraries(ejfat_grpc
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        )


set(GRPC_EXEC_FILES
        cp_tester.cc
        packetBlaster.cc
        )


# Executables
foreach(fileName ${GRPC_EXEC_FILES})
    # Get file name with no directory or extension as executable name
    get_filename_component(execName ${fileName} NAME_WE)
    # Create executable from file
    message(STATUS "Create grpc-related executable " ${execName})
    add_executable(${execName} ${fileName})
    # Put debug extension on if applicable
    set_target_properties(${execName} PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    # Location of include files
    target_include_directories(${execName} PUBLIC ${CMAKE_SOURCE_DIR} ${GRPC_INCLUDE_DIRS})
    # Needs these libs
    target_link_libraries(${execName} PUBLIC pthread ejfat_grpc)

    # Only install if installation directory has been defined
    if (DEFINED INSTALL_DIR_DEFINED)
        # Install into bin dir
        install(TARGETS ${execName} RUNTIME DESTINATION bin)
    endif()
endforeach(fileName)


# Only install if installation directory has been defined.
# CMAKE_INSTALL_PREFIX will be prepended to paths
if (DEFINED INSTALL_DIR_DEFINED)
    install(TARGETS ejfat_grpc LIBRARY DESTINATION lib)
    install(FILES ${HEADER_FILES} DESTINATION include)
endif()


# Uninstall target
if(NOT TARGET uninstall)
    configure_file(
            "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
            "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake" IMMEDIATE @ONLY)

    add_custom_target(uninstall
            COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif()

