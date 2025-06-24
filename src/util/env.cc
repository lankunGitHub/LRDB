// 环境抽象层实现

#include "lrdb/util/env.h"
#include "lrdb/core/status.h"
// thread_pool.h已删除 - 使用BackgroundManager替代
#include "lrdb/util/logging.h"
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <random>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <fstream>
#include <sstream>
#include <cstring>
#include <cstdarg>
#include <chrono>

namespace lrdb {

// POSIX环境实现
class PosixEnv : public Env {
public:
    PosixEnv() : rng_(std::random_device{}()) {}
    ~PosixEnv() override = default;
    
    // === 文件系统操作 ===
    
    Status NewSequentialFile(const std::string& filename,
                           std::unique_ptr<SequentialFile>* result) override {
        int fd = open(filename.c_str(), O_RDONLY);
        if (fd < 0) {
            return Status::IOError("Cannot open file", strerror(errno));
        }
        
        result->reset(new PosixSequentialFile(filename, fd));
        return Status::OK();
    }
    
    Status NewRandomAccessFile(const std::string& filename,
                             std::unique_ptr<RandomAccessFile>* result) override {
        int fd = open(filename.c_str(), O_RDONLY);
        if (fd < 0) {
            return Status::IOError("Cannot open file", strerror(errno));
        }
        
        result->reset(new PosixRandomAccessFile(filename, fd));
        return Status::OK();
    }
    
    Status NewWritableFile(const std::string& filename,
                         std::unique_ptr<WritableFile>* result) override {
        int fd = open(filename.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (fd < 0) {
            return Status::IOError("Cannot create file", strerror(errno));
        }
        
        result->reset(new PosixWritableFile(filename, fd));
        return Status::OK();
    }
    
    Status NewAppendableFile(const std::string& filename,
                           std::unique_ptr<WritableFile>* result) override {
        int fd = open(filename.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
        if (fd < 0) {
            return Status::IOError("Cannot open file for append", strerror(errno));
        }
        
        result->reset(new PosixWritableFile(filename, fd));
        return Status::OK();
    }
    
    bool FileExists(const std::string& filename) override {
        return access(filename.c_str(), F_OK) == 0;
    }
    
    Status GetFileAttributes(const std::string& filename,
                           FileAttributes* attrs) override {
        struct stat st;
        if (stat(filename.c_str(), &st) != 0) {
            return Status::IOError("Cannot stat file", strerror(errno));
        }
        
        attrs->size_bytes = st.st_size;
        attrs->modification_time = std::chrono::system_clock::from_time_t(st.st_mtime);
        attrs->is_directory = S_ISDIR(st.st_mode);
        attrs->is_symbolic_link = S_ISLNK(st.st_mode);
        
        return Status::OK();
    }
    
    Status DeleteFile(const std::string& filename) override {
        if (unlink(filename.c_str()) != 0) {
            return Status::IOError("Cannot delete file", strerror(errno));
        }
        return Status::OK();
    }
    
    Status RenameFile(const std::string& src, const std::string& target) override {
        if (rename(src.c_str(), target.c_str()) != 0) {
            return Status::IOError("Cannot rename file", strerror(errno));
        }
        return Status::OK();
    }
    
    Status LinkFile(const std::string& src, const std::string& target) override {
        if (link(src.c_str(), target.c_str()) != 0) {
            return Status::IOError("Cannot link file", strerror(errno));
        }
        return Status::OK();
    }
    
    Status GetFileSize(const std::string& filename, uint64_t* size) override {
        struct stat st;
        if (stat(filename.c_str(), &st) != 0) {
            return Status::IOError("Cannot stat file", strerror(errno));
        }
        *size = st.st_size;
        return Status::OK();
    }
    
    Status ReadFileToString(const std::string& filename, std::string* data) override {
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            return Status::IOError("Cannot open file for reading");
        }
        
        file.seekg(0, std::ios::end);
        std::streampos end_pos = file.tellg();
        if (end_pos < 0) {
            // tellg 失败返回 -1，赋给 size_t 会变成 SIZE_MAX 导致 bad_alloc
            return Status::IOError("Failed to determine file size");
        }
        size_t size = static_cast<size_t>(end_pos);
        file.seekg(0, std::ios::beg);
        
        data->resize(size);
        file.read(&(*data)[0], size);
        
        if (file.fail()) {
            return Status::IOError("Failed to read file");
        }
        
        return Status::OK();
    }
    
    Status WriteStringToFile(const std::string& data, const std::string& filename) override {
        std::ofstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            return Status::IOError("Cannot open file for writing");
        }
        
        file.write(data.data(), data.size());
        if (file.fail()) {
            return Status::IOError("Failed to write file");
        }
        
        return Status::OK();
    }
    
    // === 目录操作 ===
    
    Status CreateDir(const std::string& dirname) override {
        if (mkdir(dirname.c_str(), 0755) != 0) {
            return Status::IOError("Cannot create directory", strerror(errno));
        }
        return Status::OK();
    }
    
    Status CreateDirIfMissing(const std::string& dirname) override {
        if (mkdir(dirname.c_str(), 0755) != 0) {
            if (errno != EEXIST) {
                return Status::IOError("Cannot create directory", strerror(errno));
            }
        }
        return Status::OK();
    }
    
    Status DeleteDir(const std::string& dirname) override {
        if (rmdir(dirname.c_str()) != 0) {
            return Status::IOError("Cannot delete directory", strerror(errno));
        }
        return Status::OK();
    }
    
    Status GetChildren(const std::string& dirname,
                     std::vector<std::string>* result) override {
        result->clear();
        
        DIR* dir = opendir(dirname.c_str());
        if (dir == nullptr) {
            return Status::IOError("Cannot open directory", strerror(errno));
        }
        
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string name(entry->d_name);
            if (name != "." && name != "..") {
                result->push_back(name);
            }
        }
        
        closedir(dir);
        return Status::OK();
    }
    
    Status NewDirectory(const std::string& dirname,
                      std::unique_ptr<Directory>* result) override {
        int fd = open(dirname.c_str(), O_RDONLY);
        if (fd < 0) {
            return Status::IOError("Cannot open directory", strerror(errno));
        }
        
        result->reset(new PosixDirectory(fd));
        return Status::OK();
    }
    
    // === 文件锁定 ===
    
    Status LockFile(const std::string& filename,
                  std::unique_ptr<FileLock>* lock) override {
        int fd = open(filename.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
            return Status::IOError("Cannot open lock file", strerror(errno));
        }
        
        struct flock fl;
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        
        if (fcntl(fd, F_SETLK, &fl) == -1) {
            close(fd);
            return Status::IOError("Cannot lock file", strerror(errno));
        }
        
        lock->reset(new PosixFileLock(fd, filename));
        return Status::OK();
    }
    
    Status UnlockFile(std::unique_ptr<FileLock> lock) override {
        auto* posix_lock = dynamic_cast<PosixFileLock*>(lock.get());
        if (!posix_lock) {
            return Status::InvalidArgument("Invalid lock");
        }
        
        struct flock fl;
        fl.l_type = F_UNLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        
        if (fcntl(posix_lock->fd_, F_SETLK, &fl) == -1) {
            return Status::IOError("Cannot unlock file", strerror(errno));
        }

        close(posix_lock->fd_);
        posix_lock->fd_ = -1; // 析构时不再二次 close
        lock.reset();
        return Status::OK();
    }
    
    // === 时间和调度 ===
    
    uint64_t NowMicros() override {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    }
    
    uint64_t NowNanos() override {
        auto now = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
    }
    
    void SleepForMicroseconds(int micros) override {
        std::this_thread::sleep_for(std::chrono::microseconds(micros));
    }
    
    // === 线程管理 ===
    
    void StartThread(void (*function)(void* arg), void* arg) override {
        std::thread thread(function, arg);
        thread.detach();
    }
    
    // 注意：ThreadPool接口已删除，项目使用BackgroundManager进行后台任务管理
    
    // === 系统信息 ===
    
    unsigned int GetNumberOfCpus() override {
        return std::thread::hardware_concurrency();
    }
    
    uint64_t GetSystemMemory() override {
        long pages = sysconf(_SC_PHYS_PAGES);
        long page_size = sysconf(_SC_PAGE_SIZE);
        return pages * page_size;
    }
    
    uint64_t GetProcessMemoryUsage() override {
        std::ifstream statm("/proc/self/statm");
        if (!statm.is_open()) {
            return 0;
        }
        
        long rss_pages = 0;
        statm >> rss_pages >> rss_pages; // 跳过第一个字段，读取RSS
        
        long page_size = sysconf(_SC_PAGE_SIZE);
        return rss_pages * page_size;
    }
    
    // === 日志记录 ===
    
    Status NewLogger(const std::string& filename,
                   std::unique_ptr<Logger>* result) override {
        FILE* fp = fopen(filename.c_str(), "a");
        if (fp == nullptr) {
            return Status::IOError("Cannot open log file", strerror(errno));
        }
        
        result->reset(new PosixLogger(fp));
        return Status::OK();
    }
    
    // === 随机数生成 ===
    
    uint64_t GenerateRandom() override {
        std::lock_guard<std::mutex> lock(rng_mutex_);
        return rng_();
    }
    
    // === 原子操作 ===
    
    Status WriteFileAtomically(const std::string& filename,
                             const std::string& data) override {
        std::string tmp_filename = filename + ".tmp." + std::to_string(NowMicros());
        
        Status s = WriteStringToFile(data, tmp_filename);
        if (!s.ok()) {
            return s;
        }
        
        return RenameFile(tmp_filename, filename);
    }
    
    Status SyncDir(const std::string& dirname) override {
        int fd = open(dirname.c_str(), O_RDONLY);
        if (fd < 0) {
            return Status::IOError("Cannot open directory", strerror(errno));
        }
        
        if (fsync(fd) != 0) {
            close(fd);
            return Status::IOError("Cannot sync directory", strerror(errno));
        }
        
        close(fd);
        return Status::OK();
    }
    
    // === 内存映射 ===
    
    Status NewMemoryMappedFileBuffer(const std::string& filename,
                                   std::unique_ptr<MemoryMappedFileBuffer>* result) override {
        int fd = open(filename.c_str(), O_RDONLY);
        if (fd < 0) {
            return Status::IOError("Cannot open file", strerror(errno));
        }
        
        struct stat st;
        if (fstat(fd, &st) != 0) {
            close(fd);
            return Status::IOError("Cannot stat file", strerror(errno));
        }
        
        void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            close(fd);
            return Status::IOError("Cannot mmap file", strerror(errno));
        }
        
        result->reset(new PosixMemoryMappedFileBuffer(ptr, st.st_size, fd));
        return Status::OK();
    }
    
    // === 环境特定功能 ===
    
    std::string GetTempDirectory() override {
        const char* tmp = getenv("TMPDIR");
        if (tmp && *tmp != '\0') {
            return tmp;
        }
        return "/tmp";
    }
    
    std::string GetTestDirectory() override {
        return GetTempDirectory() + "/lrdb_test_" + std::to_string(getpid());
    }

private:
    // POSIX文件实现类
    class PosixSequentialFile : public SequentialFile {
    public:
        PosixSequentialFile(const std::string& filename, int fd)
            : filename_(filename), fd_(fd) {}
        
        ~PosixSequentialFile() override {
            if (fd_ >= 0) {
                close(fd_);
            }
        }
        
        Status Read(size_t n, Slice* result, char* scratch) override {
            // 循环读满 n 字节（EOF 时返回已读部分）：
            // 单次 read 的短读会被上层误判为文件损坏，掩盖真实 I/O 故障
            size_t total = 0;
            while (total < n) {
                ssize_t bytes_read = read(fd_, scratch + total, n - total);
                if (bytes_read < 0) {
                    if (errno == EINTR) continue;
                    return Status::IOError("Cannot read file", strerror(errno));
                }
                if (bytes_read == 0) {
                    break; // EOF
                }
                total += static_cast<size_t>(bytes_read);
            }
            *result = Slice(scratch, total);
            return Status::OK();
        }
        
        Status Skip(uint64_t n) override {
            if (lseek(fd_, n, SEEK_CUR) < 0) {
                return Status::IOError("Cannot seek file", strerror(errno));
            }
            return Status::OK();
        }
        
        std::string GetFileName() const override {
            return filename_;
        }
        
    private:
        std::string filename_;
        int fd_;
    };
    
    class PosixRandomAccessFile : public RandomAccessFile {
    public:
        PosixRandomAccessFile(const std::string& filename, int fd)
            : filename_(filename), fd_(fd) {}
        
        ~PosixRandomAccessFile() override {
            if (fd_ >= 0) {
                close(fd_);
            }
        }
        
        Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
            // 同 SequentialFile::Read：循环读满 n 字节，短读只在 EOF 时发生
            size_t total = 0;
            while (total < n) {
                ssize_t bytes_read =
                    pread(fd_, scratch + total, n - total, offset + total);
                if (bytes_read < 0) {
                    if (errno == EINTR) continue;
                    return Status::IOError("Cannot read file", strerror(errno));
                }
                if (bytes_read == 0) {
                    break; // EOF
                }
                total += static_cast<size_t>(bytes_read);
            }
            *result = Slice(scratch, total);
            return Status::OK();
        }
        
    private:
        std::string filename_;
        int fd_;
    };
    
    class PosixWritableFile : public WritableFile {
    public:
        PosixWritableFile(const std::string& filename, int fd)
            : filename_(filename), fd_(fd), pos_(0) {}
        
        ~PosixWritableFile() override {
            if (fd_ >= 0) {
                Close();
            }
        }
        
        Status Append(const Slice& data) override {
            const char* src = data.data();
            size_t left = data.size();
            
            while (left > 0) {
                ssize_t done = write(fd_, src, left);
                if (done < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    return Status::IOError("Cannot write file", strerror(errno));
                }
                
                left -= done;
                src += done;
                pos_ += done;
            }
            
            return Status::OK();
        }
        
        Status Close() override {
            if (fd_ >= 0) {
                if (close(fd_) != 0) {
                    return Status::IOError("Cannot close file", strerror(errno));
                }
                fd_ = -1;
            }
            return Status::OK();
        }
        
        Status Flush() override {
            return Status::OK(); // POSIX没有缓冲刷新概念
        }
        
        Status Sync() override {
            if (fsync(fd_) != 0) {
                return Status::IOError("Cannot sync file", strerror(errno));
            }
            return Status::OK();
        }
        
        uint64_t GetFileSize() override {
            return pos_;
        }
        
    private:
        std::string filename_;
        int fd_;
        uint64_t pos_;
    };
    
    class PosixDirectory : public Directory {
    public:
        explicit PosixDirectory(int fd) : fd_(fd) {}
        
        ~PosixDirectory() override {
            if (fd_ >= 0) {
                close(fd_);
            }
        }
        
        Status Fsync() override {
            if (fsync(fd_) != 0) {
                return Status::IOError("Cannot sync directory", strerror(errno));
            }
            return Status::OK();
        }
        
    private:
        int fd_;
    };
    
    class PosixFileLock : public FileLock {
    public:
        PosixFileLock(int fd, const std::string& filename)
            : fd_(fd), filename_(filename) {}

        ~PosixFileLock() override {
            // 没有走 UnlockFile 的路径（异常/忘记调用/上层 reset）也必须
            // 关闭 fd，否则泄漏文件描述符并持有锁到进程退出
            if (fd_ >= 0) {
                ::close(fd_);
                fd_ = -1;
            }
        }

        int fd_;
        std::string filename_;
    };
    
    class PosixMemoryMappedFileBuffer : public MemoryMappedFileBuffer {
    public:
        PosixMemoryMappedFileBuffer(void* base, size_t length, int fd)
            : base_(base), length_(length), fd_(fd) {}
        
        ~PosixMemoryMappedFileBuffer() override {
            if (base_ != nullptr) {
                munmap(base_, length_);
            }
            if (fd_ >= 0) {
                close(fd_);
            }
        }
        
        void* GetBase() const override { return base_; }
        size_t GetLength() const override { return length_; }
        
    private:
        void* base_;
        size_t length_;
        int fd_;
    };
    
    class PosixLogger : public Logger {
    public:
        explicit PosixLogger(FILE* fp) : fp_(fp), level_(INFO_LEVEL) {}
        
        ~PosixLogger() override {
            if (fp_) {
                fclose(fp_);
            }
        }
        
        void Logv(const char* format, va_list args) override {
            auto now = std::chrono::system_clock::now();
            auto time_t = std::chrono::system_clock::to_time_t(now);

            char time_buffer[64];
            // localtime 返回进程级静态 struct tm，多线程调用是数据竞争；
            // 用 localtime_r 写线程局部变量
            struct tm tm_buf;
            if (localtime_r(&time_t, &tm_buf) != nullptr) {
                strftime(time_buffer, sizeof(time_buffer),
                         "%Y-%m-%d %H:%M:%S", &tm_buf);
            } else {
                time_buffer[0] = '?';
                time_buffer[1] = '\0';
            }

            std::lock_guard<std::mutex> lock(mutex_);
            fprintf(fp_, "[%s] ", time_buffer);
            vfprintf(fp_, format, args);
            fprintf(fp_, "\n");
            fflush(fp_);
        }
        
        void SetLevel(Level level) override { level_ = level; }
        Level GetLevel() const override { return level_; }
        
    private:
        FILE* fp_;
        Level level_;
        std::mutex mutex_;
    };
    
    // PosixThreadPool类已删除 - 项目使用BackgroundManager替代

    // PosixEnv的私有成员变量
    
    std::mt19937_64 rng_;
    std::mutex rng_mutex_;
    
    // ThreadPool相关成员变量已删除 - 使用BackgroundManager替代
};

// 静态实例
static PosixEnv default_env;

// 全局函数实现
Env* Env::Default() {
    return &default_env;
}

std::unique_ptr<Env> NewPosixEnv() {
    return std::make_unique<PosixEnv>();
}

std::unique_ptr<Env> NewMemEnv(Env* base_env) {
    // 内存环境的简化实现
    if (!base_env) {
        base_env = Env::Default();
    }
    // 这里应该实现一个基于内存的环境，用于测试
    // 简化版本直接返回POSIX环境
    return std::make_unique<PosixEnv>();
}

} // namespace lrdb