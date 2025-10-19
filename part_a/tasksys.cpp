#include "tasksys.h"
#include <thread>

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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    workers.reserve(num_threads);
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([&]() {
            worker_loop();
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::lock_guard<std::mutex> lock(worker_mtx);
        stop_threads.store(true);
    }
    worker_cv.notify_all();
    
    for (auto& t : workers) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_loop() {
    std::unique_lock<std::mutex> lock(worker_mtx);
    while (true) {
        worker_cv.wait(lock, [this] { 
            return stop_threads || (work_available && next_task_id < current_num_total_tasks); 
        });

        if (stop_threads) {
            return;
        }
  
        lock.unlock(); 

        int task_id;
        while ((task_id = next_task_id.fetch_add(1)) < current_num_total_tasks) {
            current_runnable->runTask(task_id, current_num_total_tasks);
            tasks_completed.fetch_add(1);
        }

        lock.lock();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    if (num_total_tasks == 0) return;

    {
        std::lock_guard<std::mutex> lock(worker_mtx);

        current_runnable = runnable;
        current_num_total_tasks = num_total_tasks;
        next_task_id.store(0);
        tasks_completed.store(0);

        work_available.store(true);
    }

    worker_cv.notify_all();

    while (tasks_completed.load() < current_num_total_tasks) {
        std::this_thread::yield();
    }

    {
        std::lock_guard<std::mutex> lock(worker_mtx);
        work_available.store(false);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
