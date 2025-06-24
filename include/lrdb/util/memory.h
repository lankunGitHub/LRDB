#pragma once

#include <cstddef>
#include <cstdlib>
#include <new>

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

// 全局new/delete操作符重载。
// 注意：替换型 operator new/delete 不能声明为 inline，只能在单一 TU 定义。
// 使用方式：仅在一个 .cc 文件中 #define USE_JEMALLOC 并 #include 本头文件
#ifdef USE_JEMALLOC

// 单对象分配
void* operator new(std::size_t size) {
    void* ptr = je_malloc(size);
    if (!ptr) {
        throw std::bad_alloc();
    }
    return ptr;
}

void operator delete(void* ptr) noexcept {
    je_free(ptr);
}

// C++17 编译器会优先发出 sized delete 与 nothrow 版本；
// 不补齐会导致 je_malloc 分配的内存走 libstdc++ 默认实现（free()），堆损坏
void operator delete(void* ptr, std::size_t) noexcept {
    je_free(ptr);
}

void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    return je_malloc(size);
}

void operator delete(void* ptr, const std::nothrow_t&) noexcept {
    je_free(ptr);
}

// 数组分配
void* operator new[](std::size_t size) {
    return operator new(size);
}

void operator delete[](void* ptr) noexcept {
    operator delete(ptr);
}

// 对齐分配（C++17）
#ifdef __cpp_aligned_new
void* operator new(std::size_t size, std::align_val_t alignment) {
    void* ptr = je_aligned_alloc(static_cast<std::size_t>(alignment), size);
    if (!ptr) {
        throw std::bad_alloc();
    }
    return ptr;
}

void operator delete(void* ptr, std::align_val_t) noexcept {
    je_free(ptr);
}

void* operator new[](std::size_t size, std::align_val_t alignment) {
    return operator new(size, alignment);
}

void operator delete[](void* ptr, std::align_val_t alignment) noexcept {
    operator delete(ptr, alignment);
}
#endif

#endif // USE_JEMALLOC 