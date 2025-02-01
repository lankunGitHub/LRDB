#pragma once

#include <cstddef>
#include <cstdlib>
#include <new>

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

// 全局new/delete操作符重载
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