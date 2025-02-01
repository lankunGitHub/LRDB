// 后台任务管理器

#pragma once

#include "lrdb/core/status.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace lrdb {

// 任务优先级
enum class TaskPriority {
    kLow = 0,
    kNormal = 1,
    kHigh = 2,
    kCritical = 3
};

// 任务类型
enum class TaskType {
    kFlush,                // MemTable刷盘
    kCompaction,           // 压缩合并
    kWAL,                  // WAL操作
    kCleanup,              // 清理操作
    kMaintenance,          // 维护任务
    kStatistics,           // 统计更新
    kSnapshot,             // 快照操作
    kRecovery,             // 恢复操作
    kCustom                // 自定义任务
};

// 后台任务
struct BackgroundTask {
    TaskType type;
    TaskPriority priority;
    std::function<Status()> task_function;
    std::chrono::steady_clock::time_point schedule_time;
    std::chrono::milliseconds interval;  // 周期性任务的间隔，0表示一次性任务
    std::string name;
    uint64_t task_id;
    std::atomic<bool> cancel_requested;
    
    BackgroundTask() : type(TaskType::kCustom), priority(TaskPriority::kNormal),
                       interval(0), task_id(0), cancel_requested(false) {}
                       
    BackgroundTask(TaskType t, TaskPriority p, std::function<Status()> f,
                   const std::string& task_name = "", 
                   std::chrono::milliseconds inter = std::chrono::milliseconds(0))
        : type(t), priority(p), task_function(std::move(f)),
          schedule_time(std::chrono::steady_clock::now()),
          interval(inter), name(task_name), task_id(0), cancel_requested(false) {}

    // 拷贝构造函数
    BackgroundTask(const BackgroundTask& other)
        : type(other.type), priority(other.priority), task_function(other.task_function),
          schedule_time(other.schedule_time), interval(other.interval), 
          name(other.name), task_id(other.task_id), 
          cancel_requested(other.cancel_requested.load()) {}

    // 拷贝赋值操作符
    BackgroundTask& operator=(const BackgroundTask& other) {
        if (this != &other) {
            type = other.type;
            priority = other.priority;
            task_function = other.task_function;
            schedule_time = other.schedule_time;
            interval = other.interval;
            name = other.name;
            task_id = other.task_id;
            cancel_requested.store(other.cancel_requested.load());
        }
        return *this;
    }
};

// 后台任务管理器
class BackgroundTaskManager {
public:
    BackgroundTaskManager();
    ~BackgroundTaskManager();
    
    // 初始化和关闭
    Status Initialize(int worker_thread_count = 4);
    Status Shutdown();
    
    // 任务管理
    uint64_t ScheduleTask(const BackgroundTask& task);
    uint64_t SchedulePeriodicTask(const BackgroundTask& task);
    bool CancelTask(uint64_t task_id);
    void CancelAllTasks(TaskType type = TaskType::kCustom);
    
    // 立即执行任务
    Status ExecuteTaskImmediately(const BackgroundTask& task);
    
    // 任务查询
    bool IsTaskActive(uint64_t task_id) const;
    std::vector<uint64_t> GetActiveTasks() const;
    std::vector<uint64_t> GetTasksByType(TaskType type) const;
    
    // 暂停和恢复
    void PauseExecution();
    void ResumeExecution();
    bool IsExecutionPaused() const;
    
    // 等待所有任务完成
    Status WaitForAllTasks(std::chrono::milliseconds timeout = std::chrono::milliseconds(-1));
    
    // 设置线程数量
    Status SetWorkerThreadCount(int count);
    int GetWorkerThreadCount() const;
    
    // 批处理配置
    void SetBatchProcessingEnabled(bool enabled);
    bool IsBatchProcessingEnabled() const;
    void SetMaxBatchSize(size_t max_batch_size);
    size_t GetMaxBatchSize() const;
    void SetBatchTimeout(std::chrono::milliseconds timeout);
    std::chrono::milliseconds GetBatchTimeout() const;
    
    // 自适应调度配置
    void SetAdaptiveSchedulingEnabled(bool enabled);
    bool IsAdaptiveSchedulingEnabled() const;
    void SetAdaptiveFactors(double min_factor, double max_factor);
    std::pair<double, double> GetAdaptiveFactors() const;

private:
    // 任务比较器（用于优先队列）
    struct TaskComparator {
        bool operator()(const std::shared_ptr<BackgroundTask>& a,
                       const std::shared_ptr<BackgroundTask>& b) const {
            // 优先级高的任务优先执行
            if (a->priority != b->priority) {
                return static_cast<int>(a->priority) < static_cast<int>(b->priority);
            }
            // 同优先级按调度时间排序
            return a->schedule_time > b->schedule_time;
        }
    };
    
    std::atomic<uint64_t> next_task_id_;
    std::atomic<bool> initialized_;
    std::atomic<bool> shutdown_;
    std::atomic<bool> paused_;
    
    // 任务队列
    std::priority_queue<std::shared_ptr<BackgroundTask>,
                       std::vector<std::shared_ptr<BackgroundTask>>,
                       TaskComparator> task_queue_;
    
    // 活跃任务
    std::unordered_map<uint64_t, std::shared_ptr<BackgroundTask>> active_tasks_;
    
    // 工作线程
    std::vector<std::thread> worker_threads_;
    std::atomic<int> worker_thread_count_;
    std::atomic<int> busy_thread_count_;
    
    // 同步原语
    mutable std::mutex queue_mutex_;
    std::condition_variable task_cv_;
    std::condition_variable completion_cv_;
    
    // 批处理配置
    std::atomic<bool> batch_processing_enabled_;
    std::atomic<size_t> max_batch_size_;
    std::atomic<std::chrono::milliseconds::rep> batch_timeout_ms_;
    
    // 自适应调度配置
    std::atomic<bool> adaptive_scheduling_enabled_;
    std::atomic<double> min_adaptive_factor_;
    std::atomic<double> max_adaptive_factor_;
    
    // 内部方法
    void WorkerThreadFunction();
    std::shared_ptr<BackgroundTask> GetNextTask();
    std::vector<std::shared_ptr<BackgroundTask>> GetTaskBatch(TaskType type, size_t max_count);
    double GetAdaptiveDelayFactor() const;
    Status ExecuteTask(const std::shared_ptr<BackgroundTask>& task);
    void SchedulePeriodicTaskInternal(const std::shared_ptr<BackgroundTask>& task);
    uint64_t GenerateTaskID();
    void RemoveTask(uint64_t task_id);
    
    // 禁止拷贝
    BackgroundTaskManager(const BackgroundTaskManager&) = delete;
    BackgroundTaskManager& operator=(const BackgroundTaskManager&) = delete;
};

// 工具函数
BackgroundTask CreatePeriodicTask(TaskType type,
                                 std::function<Status()> task_function,
                                 std::chrono::milliseconds interval,
                                 TaskPriority priority = TaskPriority::kNormal);

}  // namespace lrdb
