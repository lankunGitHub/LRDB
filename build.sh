#!/bin/bash
# LRDB构建脚本

set -e  # 遇到错误时退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_message() {
    echo -e "${2}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

# 检查依赖
check_dependencies() {
    print_message "检查构建依赖..." ${BLUE}
    
    # 检查CMake
    if ! command -v cmake &> /dev/null; then
        print_message "错误: 未找到CMake，请先安装CMake" ${RED}
        exit 1
    fi
    
    # 检查编译器
    if ! command -v g++ &> /dev/null && ! command -v clang++ &> /dev/null; then
        print_message "错误: 未找到C++编译器，请先安装g++或clang++" ${RED}
        exit 1
    fi
    
    # 检查jemalloc
    if ! pkg-config --exists jemalloc; then
        print_message "警告: 未找到jemalloc，正在尝试安装..." ${YELLOW}
        # 尝试自动安装jemalloc
        if command -v apt-get &> /dev/null; then
            sudo apt-get update && sudo apt-get install -y libjemalloc-dev
        elif command -v yum &> /dev/null; then
            sudo yum install -y jemalloc-devel
        elif command -v pacman &> /dev/null; then
            sudo pacman -S jemalloc
        else
            print_message "错误: 无法自动安装jemalloc，请手动安装" ${RED}
            exit 1
        fi
    fi
    
    print_message "依赖检查完成" ${GREEN}
}

# 清理构建目录
clean_build() {
    print_message "清理构建目录..." ${BLUE}
    rm -rf build
    print_message "构建目录已清理" ${GREEN}
}

# 配置构建
configure_build() {
    print_message "配置构建..." ${BLUE}
    
    # 创建构建目录
    mkdir -p build
    cd build
    
    # 设置构建类型
    BUILD_TYPE=${1:-Release}
    print_message "构建类型: $BUILD_TYPE" ${BLUE}
    
    # CMake配置
    cmake .. \
        -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_CXX_STANDARD_REQUIRED=ON \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
    
    print_message "构建配置完成" ${GREEN}
}

# 编译项目
build_project() {
    print_message "开始编译项目..." ${BLUE}
    
    # 获取CPU核心数
    NPROC=$(nproc 2>/dev/null || echo 4)
    print_message "使用 $NPROC 个并行任务进行编译" ${BLUE}
    
    # 编译
    make -j$NPROC
    
    print_message "项目编译完成" ${GREEN}
}

# 运行测试
run_tests() {
    print_message "运行测试..." ${BLUE}
    
    if [ -f "test/test_basic" ]; then
        ./test/test_basic
        print_message "基础测试通过" ${GREEN}
    else
        print_message "未找到测试可执行文件" ${YELLOW}
    fi
    
    if [ -f "test/test_units" ]; then
        ./test/test_units
        print_message "单元测试通过" ${GREEN}
    else
        print_message "未找到单元测试可执行文件" ${YELLOW}
    fi
}

# 安装项目
install_project() {
    print_message "安装项目..." ${BLUE}
    
    make install
    
    print_message "项目安装完成" ${GREEN}
}

# 显示帮助
show_help() {
    echo "LRDB构建脚本使用说明："
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help          显示此帮助信息"
    echo "  -c, --clean         清理构建目录"
    echo "  -d, --debug         使用Debug模式构建"
    echo "  -r, --release       使用Release模式构建 (默认)"
    echo "  -t, --test          编译后运行测试"
    echo "  -i, --install       安装项目到系统"
    echo "  --check-deps        只检查依赖"
    echo ""
    echo "示例:"
    echo "  $0                  默认Release构建"
    echo "  $0 -d              Debug构建"
    echo "  $0 -c -r -t        清理、Release构建并测试"
    echo "  $0 --check-deps     只检查依赖"
}

# 主函数
main() {
    print_message "LRDB构建脚本启动" ${BLUE}
    print_message "版本: 1.0.0" ${BLUE}
    
    # 解析命令行参数
    BUILD_TYPE="Release"
    CLEAN=false
    TEST=false
    INSTALL=false
    CHECK_DEPS_ONLY=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -c|--clean)
                CLEAN=true
                shift
                ;;
            -d|--debug)
                BUILD_TYPE="Debug"
                shift
                ;;
            -r|--release)
                BUILD_TYPE="Release"
                shift
                ;;
            -t|--test)
                TEST=true
                shift
                ;;
            -i|--install)
                INSTALL=true
                shift
                ;;
            --check-deps)
                CHECK_DEPS_ONLY=true
                shift
                ;;
            *)
                print_message "未知选项: $1" ${RED}
                show_help
                exit 1
                ;;
        esac
    done
    
    # 检查依赖
    check_dependencies
    
    if [ "$CHECK_DEPS_ONLY" = true ]; then
        print_message "依赖检查完成，退出" ${GREEN}
        exit 0
    fi
    
    # 记录开始时间
    START_TIME=$(date +%s)
    
    # 执行构建流程
    if [ "$CLEAN" = true ]; then
        clean_build
    fi
    
    configure_build $BUILD_TYPE
    build_project
    
    if [ "$TEST" = true ]; then
        run_tests
    fi
    
    if [ "$INSTALL" = true ]; then
        install_project
    fi
    
    # 计算构建时间
    END_TIME=$(date +%s)
    BUILD_TIME=$((END_TIME - START_TIME))
    
    print_message "构建完成！总用时: ${BUILD_TIME}秒" ${GREEN}
    
    # 显示构建结果
    print_message "构建产物:" ${BLUE}
    echo "  - 静态库: build/liblrdb.a"
    if [ -f "build/test/test_basic" ]; then
        echo "  - 基础测试: build/test/test_basic"
    fi
    if [ -f "build/test/test_units" ]; then
        echo "  - 单元测试: build/test/test_units"
    fi
    if [ -f "build/examples/basic_usage" ]; then
        echo "  - 基础示例: build/examples/basic_usage"
    fi
    
    print_message "使用说明:" ${BLUE}
    echo "  - 运行基础测试: ./build/test/test_basic"
    echo "  - 运行单元测试: ./build/test/test_units"
    echo "  - 查看示例: ./build/examples/basic_usage"
    echo "  - 性能测试: ./build/test/test_benchmark 100000"
}

# 确保脚本从项目根目录运行
if [ ! -f "CMakeLists.txt" ]; then
    print_message "错误: 请从项目根目录运行此脚本" ${RED}
    exit 1
fi

# 执行主函数
main "$@"