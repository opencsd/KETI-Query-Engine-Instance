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

include(./build/cmake/common.cmake)

# find_package(MySQLConnectorCPP REQUIRED)

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

# Query_Engine_Instance
add_executable(query_engine_instance "main.cc" "storage_engine_interface.h" "plan_executor.cc" "plan_executor.h" "query_planner.h" "meta_data_manager.h" "parsed_query.h" "db_connector_instance.cpp" "db_connector_instance.h" "query_log_assistant.h" "stdafx.h" "kodbc.h" "keti_log.h")

set(Casablanca_LIBRARIES "-lboost_system -lcrypto -lssl -lcpprest")
set(KODBC_LLIBRARIES "-lodbc")
set(LINKING_LIBRARIES ${Casablanca_LIBRARIES} ${KODBC_LLIBRARIES})
set(MySQLConnectorCPP_INCLUDE_DIRS "-lmysqlcppconn")

if (UNIX)
	# Linking POSIX threads
	find_package(Threads REQUIRED)
	set(LINKING_LIBRARIES ${LINKING_LIBRARIES} Threads::Threads)
endif (UNIX)

# target_include_directories(query_engine_instance PRIVATE ${MySQLConnectorCPP_INCLUDE_DIRS})

target_link_libraries(query_engine_instance
  ss_grpc_proto
  ${MySQLConnectorCPP_INCLUDE_DIRS}
  ${LINKING_LIBRARIES}
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})