// LRDB轻量测试框架 - 无外部依赖的断言与统计
#pragma once

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace lrdb_test {

template <typename T> inline std::string ToString(const T &v) {
  std::ostringstream oss;
  oss << v;
  return oss.str();
}

inline std::string ToString(const char *v) { return v ? v : "(null)"; }

struct TestCase {
    std::string name;
    std::function<void()> func;
};

inline std::vector<TestCase>& GetTestCases() {
    static std::vector<TestCase> cases;
    return cases;
}

inline int g_failures = 0;

struct TestRegistrar {
    TestRegistrar(const std::string& name, std::function<void()> func) {
        GetTestCases().push_back(TestCase{name, std::move(func)});
    }
};

// 断言宏：失败时记录并抛出异常终止当前用例
#define TEST_EXPECT(cond, msg)                                                 \
  do {                                                                         \
    if (!(cond)) {                                                             \
      ::lrdb_test::g_failures++;                                               \
      std::fprintf(stderr, "  [FAIL] %s:%d: %s\n", __FILE__, __LINE__, msg);   \
      throw std::runtime_error(msg);                                           \
    }                                                                          \
  } while (0)

#define TEST_EXPECT_EQ(a, b)                                                   \
  do {                                                                         \
    auto _a = (a);                                                             \
    auto _b = (b);                                                             \
    if (!(_a == _b)) {                                                         \
      ::lrdb_test::g_failures++;                                               \
      std::fprintf(stderr, "  [FAIL] %s:%d: expect equal (%s vs %s)\n",        \
                   __FILE__, __LINE__,                                         \
                   ::lrdb_test::ToString(_a).c_str(),                          \
                   ::lrdb_test::ToString(_b).c_str());                         \
      throw std::runtime_error("expect equal");                                \
    }                                                                          \
  } while (0)

#define TEST(name)                                                             \
  static void test_##name();                                                   \
  static ::lrdb_test::TestRegistrar registrar_##name(#name, test_##name);      \
  static void test_##name()

// 运行所有已注册的用例
inline int RunAllTests(const std::string& suite_name) {
    int passed = 0;
    int failed = 0;
    std::printf("=== %s ===\n", suite_name.c_str());

    for (const auto& tc : GetTestCases()) {
        std::printf("[ RUN ] %s\n", tc.name.c_str());
        try {
            tc.func();
            std::printf("[ OK  ] %s\n", tc.name.c_str());
            ++passed;
        } catch (const std::exception& e) {
            std::printf("[FAILED] %s: %s\n", tc.name.c_str(), e.what());
            ++failed;
        } catch (...) {
            std::printf("[FAILED] %s: unknown exception\n", tc.name.c_str());
            ++failed;
        }
    }

    std::printf("=== %s: %d passed, %d failed ===\n", suite_name.c_str(),
                passed, failed);
    return failed == 0 ? 0 : 1;
}

}  // namespace lrdb_test
