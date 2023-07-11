# Copyright 2021 the gRPC authors.
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
# cmake build file for C++ keyvaluestore example.
# Assumes protobuf and gRPC have been installed using cmake.
# See cmake_externalproject/CMakeLists.txt for all-in-one cmake build
# that automatically builds all the dependencies before building keyvaluestore.

cmake_minimum_required(VERSION 3.5.1)

project(snippet_sample C CXX)

include(../cmake/common.cmake)

# Proto file
get_filename_component(ss_proto "./proto/snippet_sample.proto" ABSOLUTE)
get_filename_component(ss_proto_path "${ss_proto}" PATH)

# Generated sources
set(ss_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.pb.cc")
set(ss_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.pb.h")
set(ss_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.grpc.pb.cc")
set(ss_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/snippet_sample.grpc.pb.h")
add_custom_command(
      OUTPUT "${ss_proto_srcs}" "${ss_proto_hdrs}" "${ss_grpc_srcs}" "${ss_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${ss_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${ss_proto}"
      DEPENDS "${ss_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# kvs_grpc_proto
add_library(ss_grpc_proto
  ${ss_grpc_srcs}
  ${ss_grpc_hdrs}
  ${ss_proto_srcs}
  ${ss_proto_hdrs})
target_link_libraries(ss_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# DB_Connector_Instance
add_executable(db_connector_instance "DB_Connector_Instance_Main.cc" "Storage_Engine_Interface.h" "Plan_Executer.cc" "Plan_Executer.h" "Query_Planner.h" "Meta_Data_Manager.h" "Parsed_Query.h" "DB_Connector_Instance.cpp" "DB_Connector_Instance.h" "stdafx.h" "kodbc.h" "keti_log.h")

set(Casablanca_LIBRARIES "-lboost_system -lcrypto -lssl -lcpprest")
set(KODBC_LLIBRARIES "-lodbc")
set(LINKING_LIBRARIES ${Casablanca_LIBRARIES} ${KODBC_LLIBRARIES})

if (UNIX)
	# Linking POSIX threads
	find_package(Threads REQUIRED)
	set(LINKING_LIBRARIES ${LINKING_LIBRARIES} Threads::Threads)
endif (UNIX)

target_link_libraries(db_connector_instance
  ss_grpc_proto
  ${LINKING_LIBRARIES}
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Storage_Engine_Instance
add_executable(storage_engine_instance "storage_engine_instance.cc")
target_link_libraries(storage_engine_instance
  ss_grpc_proto
  ${LINKING_LIBRARIES}
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
