cmake_minimum_required(VERSION 3.16)
project(fleet_task_assignment CXX)

# Standalone on purpose: this extension needs Postgres (via libpqxx) and
# nothing else, so its build does not depend on the sibling datagen
# control plane's gRPC/protobuf toolchain at all.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED libpqxx)
find_package(GTest REQUIRED)

add_library(fleet_core
  src/task_state.cpp
  src/fleet_store.cpp
)
target_include_directories(fleet_core PUBLIC src ${PQXX_INCLUDE_DIRS})
target_link_libraries(fleet_core PUBLIC ${PQXX_LIBRARIES} Threads::Threads)

add_executable(fleet_coordinator src/fleet_coordinator.cpp)
target_link_libraries(fleet_coordinator PRIVATE fleet_core)

add_executable(fleet_tool tools/fleet_tool.cpp)
target_link_libraries(fleet_tool PRIVATE fleet_core)

enable_testing()
add_executable(fleet_unit_tests tests/test_fleet_store.cpp)
target_link_libraries(fleet_unit_tests PRIVATE fleet_core GTest::GTest GTest::Main)
add_test(NAME fleet_unit_tests COMMAND fleet_unit_tests)
