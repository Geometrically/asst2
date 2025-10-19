#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) : num_threads(num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks == 0) {
        return;
    }

    std::vector<std::thread> threads;
    std::atomic<int> next_task_id(0);

    // Launch worker threads
    int num_worker_threads = std::min(num_threads, num_total_tasks);
    threads.reserve(num_worker_threads);

    for (int i = 0; i < num_worker_threads; ++i) {
        threads.emplace_back([&]() {
            // Each thread loops until all tasks are claimed
            int task_id;
            while ((task_id = next_task_id.fetch_add(1)) < num_total_tasks) {
                runnable->runTask(task_id, num_total_tasks);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    run(runnable, num_total_tasks);

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.

    workers.reserve(num_threads);
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([&]() {
            worker_loop();
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop_threads.store(true);

    for (auto& t : workers) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::worker_loop() {
    while (!stop_threads) {
        if (work_available) {
            int task_id;
            while ((task_id = next_task_id.fetch_add(1)) < current_num_total_tasks) {
                current_runnable->runTask(task_id, current_num_total_tasks);
                tasks_completed.fetch_add(1);
            }
        }
        std::this_thread::yield();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    if (num_total_tasks == 0) return;

    current_runnable = runnable;
    current_num_total_tasks = num_total_tasks;
    next_task_id.store(0);
    tasks_completed.store(0);

    work_available.store(true);

    while (tasks_completed.load() < current_num_total_tasks) {
        std::this_thread::yield();
    }

    work_available.store(false);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    run(runnable, num_total_tasks);

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads),
    stop_threads(false), 
    max_task_id(0), 
    total_outstanding_tasks_(0) 
{
    workers.reserve(num_threads);
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([&]() {
            worker_loop();
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(worker_mtx);
        stop_threads.store(true);
    }
    worker_cv.notify_all();
    sync_cv_.notify_all();
    
    for (auto& t : workers) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_loop() {
    while (true) {
        std::unique_lock<std::mutex> lock(worker_mtx);
        
        worker_cv.wait(lock, [this] { 
            return stop_threads || !ready_queue_.empty(); 
        });

        if (stop_threads) {
            return;
        }

        TaskID task_id = ready_queue_.front();
        Task& task = queued_tasks.at(task_id);

        int instance_id = task.next_instance_id.fetch_add(1);

        if (instance_id < task.count) {
            lock.unlock(); 
            
            task.runnable->runTask(instance_id, task.count);
            
            int completed_count = task.completed_instances.fetch_add(1) + 1;
            
            if (completed_count == task.count) {
                std::lock_guard<std::mutex> guard(worker_mtx);
                
                for (TaskID dependent_task_id : task_dependencies[task_id]) {
                    Task& dependent_task = queued_tasks.at(dependent_task_id);
                    
                    int remaining_deps = dependent_task.outstanding_dependencies.fetch_sub(1) - 1;
                    
                    if (remaining_deps == 0) {
                        ready_queue_.push_back(dependent_task_id);
                        worker_cv.notify_all();
                    }
                }
                
                int outstanding = total_outstanding_tasks_.fetch_sub(1) - 1;
                if (outstanding == 0) {
                    sync_cv_.notify_all();
                }
            }
        } else {
            if (instance_id == task.count) {
                if (!ready_queue_.empty() && ready_queue_.front() == task_id) {
                    ready_queue_.pop_front();
                }
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    if (num_total_tasks == 0) return;

    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    if (num_total_tasks == 0) return 0;
    
    std::lock_guard<std::mutex> lock(worker_mtx);
    
    TaskID new_task_id = ++max_task_id;
    
    total_outstanding_tasks_.fetch_add(1);

    queued_tasks.emplace(std::piecewise_construct,
                     std::forward_as_tuple(new_task_id),
                     std::forward_as_tuple(new_task_id, num_total_tasks, runnable, deps.size()));
    
    for (TaskID dep_id : deps) {
        task_dependencies[dep_id].push_back(new_task_id);
    }
    
    if (deps.empty()) {
        ready_queue_.push_back(new_task_id);
        worker_cv.notify_all();
    }

    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(worker_mtx);
    
    sync_cv_.wait(lock, [this] {
        return total_outstanding_tasks_.load() == 0;
    });
}
