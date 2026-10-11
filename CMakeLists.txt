cmake_minimum_required(VERSION 3.16)
project(datagen_control_plane CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# This machine has two protoc installs (a newer one ahead on PATH, and the
# apt one matching the apt-installed libprotobuf-dev / grpc++ runtime this
# project actually links against). Codegen must use the apt one or the
# generated code and the runtime library disagree on wire format internals.
set(PROTOC_BIN /usr/bin/protoc)
set(GRPC_CPP_PLUGIN /usr/bin/grpc_cpp_plugin)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
pkg_check_modules(GRPCPP REQUIRED grpc++)
pkg_check_modules(PQXX REQUIRED libpqxx)
find_package(OpenSSL REQUIRED)
find_package(GTest REQUIRED)

set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
set(GEN_DIR ${CMAKE_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN_DIR})

set(PROTO_SRCS ${GEN_DIR}/control_plane.pb.cc)
set(PROTO_HDRS ${GEN_DIR}/control_plane.pb.h)
set(GRPC_SRCS ${GEN_DIR}/control_plane.grpc.pb.cc)
set(GRPC_HDRS ${GEN_DIR}/control_plane.grpc.pb.h)

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND ${PROTOC_BIN}
          --proto_path=${PROTO_DIR}
          --cpp_out=${GEN_DIR}
          --grpc_out=${GEN_DIR}
          --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
          ${PROTO_DIR}/control_plane.proto
  DEPENDS ${PROTO_DIR}/control_plane.proto
  COMMENT "Generating protobuf/gRPC C++ code"
)
add_custom_target(proto_gen DEPENDS ${PROTO_SRCS} ${GRPC_SRCS})

add_library(control_plane_core
  src/dag.cpp
  src/fair_share.cpp
  src/store.cpp
  ${PROTO_SRCS}
  ${GRPC_SRCS}
)
add_dependencies(control_plane_core proto_gen)
target_include_directories(control_plane_core PUBLIC
  src ${GEN_DIR} ${PROTOBUF_INCLUDE_DIRS} ${GRPCPP_INCLUDE_DIRS} ${PQXX_INCLUDE_DIRS}
)
target_link_libraries(control_plane_core PUBLIC
  ${PROTOBUF_LIBRARIES} ${GRPCPP_LIBRARIES} ${PQXX_LIBRARIES} OpenSSL::Crypto Threads::Threads
)
target_compile_options(control_plane_core PUBLIC ${PROTOBUF_CFLAGS_OTHER} ${GRPCPP_CFLAGS_OTHER})

add_executable(coordinator src/coordinator_server.cpp)
target_link_libraries(coordinator PRIVATE control_plane_core)

add_executable(control_plane_cli tools/client_tool.cpp)
target_link_libraries(control_plane_cli PRIVATE control_plane_core)

enable_testing()
add_executable(unit_tests
  tests/test_dag.cpp
  tests/test_fair_share.cpp
  tests/test_store.cpp
)
target_link_libraries(unit_tests PRIVATE control_plane_core GTest::GTest GTest::Main)
add_test(NAME unit_tests COMMAND unit_tests)
