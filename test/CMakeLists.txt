# CMakeLists.txt for LRDB tests

cmake_minimum_required(VERSION 3.10)

# 基础功能测试
add_executable(test_basic test_basic.cc)
target_link_libraries(test_basic lrdb_static)
target_include_directories(test_basic PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
set_property(TARGET test_basic PROPERTY CXX_STANDARD 17)

# 组件单元测试
add_executable(test_units test_units.cc)
target_link_libraries(test_units lrdb_static)
target_include_directories(test_units PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
set_property(TARGET test_units PROPERTY CXX_STANDARD 17)

# 性能基准
add_executable(test_benchmark test_benchmark.cc)
target_link_libraries(test_benchmark lrdb_static)
target_include_directories(test_benchmark PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR})
set_property(TARGET test_benchmark PROPERTY CXX_STANDARD 17)
