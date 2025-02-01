// 后台任务管理器实现

#include "lrdb/background/background_manager.h"
#include "lrdb/util/logging.h"

#include <algorithm>
#include <chrono>

namespace lrdb {

// BackgroundTaskManager实现
BackgroundTaskManager::BackgroundTaskManager()
    : next_task_id_(1), initialized_(false), shutdown_(false), paused_(false),
      worker_thread_count_(0), busy_thread_count_(0),
      batch_processing_enabled_(true), max_batch_size_(5),
      batch_timeout_ms_(std::chrono::milliseconds(100).count()),
      adaptive_scheduling_enabled_(false),
      min_adaptive_factor_(0.5), max_adaptive_factor_(2.0) {}

BackgroundTaskManager::~BackgroundTaskManager() {
    if (initialized_.load() && !shutdown_.load()) {
        Shutdown();
    }
}

Status BackgroundTaskManager::Initialize(int worker_thread_count) {
    if (initialized_.load()) {
        return Status::OK();
    }

    if (worker_thread_count <= 0) {
        worker_thread_count = std::thread::hardware_concurrency();
        if (worker_thread_count == 0) {
            worker_thread_count = 4; // 默认值
        }
    }

    worker_thread_count_.store(worker_thread_count);
    
    // 启动工作线程
    worker_threads_.reserve(worker_thread_count);
    for (int i = 0; i < worker_thread_count; ++i) {
        worker_threads_.emplace_back(&BackgroundTaskManager::WorkerThreadFunction, this);
    }

    initialized_.store(true);
    LOG_INFO << "BackgroundTaskManager initialized with " << worker_thread_count << " worker threads";
    
    return Status::OK();
}

Status BackgroundTaskManager::Shutdown() {
    if (shutdown_.load()) {
        return Status::OK();
    }

    LOG_INFO << "BackgroundTaskManager shutting down...";
    
    shutdown_.store(true);
    task_cv_.notify_all();

    // 等待所有工作线程完成
    for (auto& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    worker_threads_.clear();

    // 清理队列
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        while (!task_queue_.empty()) {
            task_queue_.pop();
        }
        active_tasks_.clear();
    }

    initialized_.store(false);
    LOG_INFO << "BackgroundTaskManager shutdown complete";

    return Status::OK();
}

uint64_t BackgroundTaskManager::ScheduleTask(const BackgroundTask& task) {
    if (!initialized_.load() || shutdown_.load()) {
        LOG_WARN << "Cannot schedule task: BackgroundTaskManager not initialized or shutdown";
        return 0;
    }

    auto task_ptr = std::make_shared<BackgroundTask>(task);
    task_ptr->task_id = next_task_id_.fetch_add(1);
    task_ptr->schedule_time = std::chrono::steady_clock::now();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push(task_ptr);
        active_tasks_[task_ptr->task_id] = task_ptr;
    }

    task_cv_.notify_one();
    return task_ptr->task_id;
}

uint64_t BackgroundTaskManager::SchedulePeriodicTask(const BackgroundTask& task) {
    auto task_id = ScheduleTask(task);
    if (task_id > 0) {
        LOG_INFO << "Scheduled periodic task " << task_id << " with interval " 
                 << task.interval.count() << "ms";
    }
    return task_id;
}

bool BackgroundTaskManager::CancelTask(uint64_t task_id) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    auto it = active_tasks_.find(task_id);
    if (it != active_tasks_.end()) {
        it->second->cancel_requested.store(true);
        return true;
    }
    
    return false;
}

void BackgroundTaskManager::CancelAllTasks(TaskType type) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    for (auto& pair : active_tasks_) {
        if (pair.second->type == type) {
            pair.second->cancel_requested.store(true);
        }
    }
}

Status BackgroundTaskManager::ExecuteTaskImmediately(const BackgroundTask& task) {
    if (!initialized_.load() || shutdown_.load()) {
        return Status::InvalidArgument("BackgroundTaskManager not initialized or shutdown");
    }

    try {
        LOG_INFO << "Executing immediate task: " << task.name;
        auto status = task.task_function();
        LOG_INFO << "Immediate task completed: " << task.name 
                 << " Status: " << status.ToString();
        return status;
    } catch (const std::exception& e) {
        LOG_ERROR << "Exception in immediate task " << task.name << ": " << e.what();
        return Status::IOError("Task execution failed: " + std::string(e.what()));
    }
}

bool BackgroundTaskManager::IsTaskActive(uint64_t task_id) const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return active_tasks_.find(task_id) != active_tasks_.end();
}

std::vector<uint64_t> BackgroundTaskManager::GetActiveTasks() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    std::vector<uint64_t> task_ids;
    task_ids.reserve(active_tasks_.size());
    
    for (const auto& pair : active_tasks_) {
        task_ids.push_back(pair.first);
    }
    
    return task_ids;
}

std::vector<uint64_t> BackgroundTaskManager::GetTasksByType(TaskType type) const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    std::vector<uint64_t> task_ids;
    
    for (const auto& pair : active_tasks_) {
        if (pair.second->type == type) {
            task_ids.push_back(pair.first);
        }
    }
    
    return task_ids;
}

void BackgroundTaskManager::PauseExecution() {
    paused_.store(true);
    LOG_INFO << "BackgroundTaskManager execution paused";
}

void BackgroundTaskManager::ResumeExecution() {
    paused_.store(false);
    task_cv_.notify_all();
    LOG_INFO << "BackgroundTaskManager execution resumed";
}

bool BackgroundTaskManager::IsExecutionPaused() const {
    return paused_.load();
}

Status BackgroundTaskManager::WaitForAllTasks(std::chrono::milliseconds timeout) {
    auto start = std::chrono::steady_clock::now();
    
    while (true) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (task_queue_.empty() && active_tasks_.empty()) {
                return Status::OK();
            }
        }
        
        if (timeout.count() > 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            if (elapsed >= timeout) {
                return Status::TimedOut("Timeout waiting for tasks to complete");
            }
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

Status BackgroundTaskManager::SetWorkerThreadCount(int count) {
    if (count <= 0) {
        return Status::InvalidArgument("Worker thread count must be positive");
    }
    
    if (initialized_.load()) {
        return Status::InvalidArgument("Cannot change thread count after initialization");
    }
    
    worker_thread_count_.store(count);
    return Status::OK();
}

int BackgroundTaskManager::GetWorkerThreadCount() const {
    return worker_thread_count_.load();
}

void BackgroundTaskManager::SetBatchProcessingEnabled(bool enabled) {
    batch_processing_enabled_.store(enabled);
}

bool BackgroundTaskManager::IsBatchProcessingEnabled() const {
    return batch_processing_enabled_.load();
}

void BackgroundTaskManager::SetMaxBatchSize(size_t max_batch_size) {
    max_batch_size_.store(max_batch_size);
}

size_t BackgroundTaskManager::GetMaxBatchSize() const {
    return max_batch_size_.load();
}

void BackgroundTaskManager::SetBatchTimeout(std::chrono::milliseconds timeout) {
    batch_timeout_ms_.store(timeout.count());
}

std::chrono::milliseconds BackgroundTaskManager::GetBatchTimeout() const {
    return std::chrono::milliseconds(batch_timeout_ms_.load());
}

void BackgroundTaskManager::SetAdaptiveSchedulingEnabled(bool enabled) {
    adaptive_scheduling_enabled_.store(enabled);
}

bool BackgroundTaskManager::IsAdaptiveSchedulingEnabled() const {
    return adaptive_scheduling_enabled_.load();
}

void BackgroundTaskManager::SetAdaptiveFactors(double min_factor, double max_factor) {
    min_adaptive_factor_.store(min_factor);
    max_adaptive_factor_.store(max_factor);
}

std::pair<double, double> BackgroundTaskManager::GetAdaptiveFactors() const {
    return {min_adaptive_factor_.load(), max_adaptive_factor_.load()};
}

// 私有方法实现

void BackgroundTaskManager::WorkerThreadFunction() {
    while (!shutdown_.load()) {
        // 检查是否暂停
        if (paused_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        
        auto task = GetNextTask();
        if (!task) {
            // 如果没有任务，短暂休眠避免忙等待
            double delay_factor = GetAdaptiveDelayFactor();
            auto delay = std::chrono::milliseconds(static_cast<int>(100 * delay_factor));
            
            std::unique_lock<std::mutex> lock(queue_mutex_);
            task_cv_.wait_for(lock, delay, [this] {
                return !task_queue_.empty() || shutdown_.load();
            });
            continue;
        }

        // 检查任务是否被取消
        if (task->cancel_requested.load()) {
            RemoveTask(task->task_id);
            continue;
        }

        // 执行任务
        ExecuteTask(task);
    }
}

std::shared_ptr<BackgroundTask> BackgroundTaskManager::GetNextTask() {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    if (task_queue_.empty()) {
        return nullptr;
    }
    
    auto task = task_queue_.top();
    task_queue_.pop();
    
    return task;
}

std::vector<std::shared_ptr<BackgroundTask>> BackgroundTaskManager::GetTaskBatch(TaskType type, size_t max_count) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    std::vector<std::shared_ptr<BackgroundTask>> batch;
    size_t max_batch = std::min(max_count, max_batch_size_.load());
    
    // 临时存储不匹配的任务
    std::vector<std::shared_ptr<BackgroundTask>> temp_tasks;
    
    while (!task_queue_.empty() && batch.size() < max_batch) {
        auto task = task_queue_.top();
        task_queue_.pop();
        
        if (task->type == type && !task->cancel_requested.load()) {
            batch.push_back(task);
        } else {
            temp_tasks.push_back(task);
        }
    }
    
    // 将不匹配的任务放回队列
    for (auto& task : temp_tasks) {
        task_queue_.push(task);
    }
    
    return batch;
}

Status BackgroundTaskManager::ExecuteTask(const std::shared_ptr<BackgroundTask>& task) {
    busy_thread_count_.fetch_add(1);
    
    auto start_time = std::chrono::steady_clock::now();
    
    Status status;
    try {
        LOG_DEBUG << "Executing task: " << task->name << " (ID: " << task->task_id << ")";
        status = task->task_function();
        LOG_DEBUG << "Task completed: " << task->name << " Status: " << status.ToString();
    } catch (const std::exception& e) {
        status = Status::IOError("Task execution failed: " + std::string(e.what()));
        LOG_ERROR << "Exception in task " << task->name << ": " << e.what();
    }
    
    // 从活跃任务中移除（除非是周期性任务）
    if (task->interval.count() == 0 || !status.ok()) {
        RemoveTask(task->task_id);
    } else {
        // 重新调度周期性任务
        SchedulePeriodicTaskInternal(task);
    }
    
    busy_thread_count_.fetch_sub(1);
    return status;
}

void BackgroundTaskManager::RemoveTask(uint64_t task_id) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    active_tasks_.erase(task_id);
    completion_cv_.notify_all();
}

double BackgroundTaskManager::GetAdaptiveDelayFactor() const {
    if (!adaptive_scheduling_enabled_.load()) {
        return 1.0;
    }
    
    // 简化的自适应延迟计算，基于线程利用率
    double thread_utilization = static_cast<double>(busy_thread_count_.load()) / 
                                static_cast<double>(worker_thread_count_.load());
    
    double min_factor = min_adaptive_factor_.load();
    double max_factor = max_adaptive_factor_.load();
    
    // 基于线程利用率调整延迟因子
    if (thread_utilization > 0.8) {
        return max_factor;
    } else if (thread_utilization < 0.2) {
        return min_factor;
    } else {
        return min_factor + (max_factor - min_factor) * thread_utilization;
    }
}

void BackgroundTaskManager::SchedulePeriodicTaskInternal(
    const std::shared_ptr<BackgroundTask>& task) {
    
    // 计算下次执行时间，应用自适应延迟
    auto base_interval = task->interval;
    double delay_factor = GetAdaptiveDelayFactor();
    auto adjusted_interval = std::chrono::milliseconds(
        static_cast<int>(base_interval.count() * delay_factor));
    
    // 创建新的任务实例用于下次执行
    auto next_task = std::make_shared<BackgroundTask>(*task);
    next_task->task_id = next_task_id_.fetch_add(1);
    next_task->schedule_time = std::chrono::steady_clock::now() + adjusted_interval;
    next_task->cancel_requested.store(false);
    
    // 延迟调度
    std::thread([this, next_task, adjusted_interval]() {
        std::this_thread::sleep_for(adjusted_interval);
        
        if (!shutdown_.load() && !next_task->cancel_requested.load()) {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            task_queue_.push(next_task);
            active_tasks_[next_task->task_id] = next_task;
            task_cv_.notify_one();
        }
    }).detach();
}

uint64_t BackgroundTaskManager::GenerateTaskID() {
    return next_task_id_.fetch_add(1);
}

// 工具函数实现

BackgroundTask CreatePeriodicTask(TaskType type,
                                 std::function<Status()> task_function,
                                 std::chrono::milliseconds interval,
                                 TaskPriority priority) {
    return BackgroundTask(type, priority, std::move(task_function), 
                         "periodic_task", interval);
}

}  // namespace lrdb