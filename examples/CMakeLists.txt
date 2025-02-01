# CMakeLists.txt for LRDB examples

cmake_minimum_required(VERSION 3.16)

# 添加示例程序
add_executable(basic_usage basic_usage.cc)

# 链接到静态库目标
target_link_libraries(basic_usage lrdb_static)

# 使用工程 include 目录
target_include_directories(basic_usage PRIVATE ${CMAKE_SOURCE_DIR}/include)

# 设置C++标准
set_property(TARGET basic_usage PROPERTY CXX_STANDARD 17)

# 添加安装规则
install(TARGETS basic_usage DESTINATION bin)
