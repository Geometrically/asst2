#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <unordered_map>
#include <thread>
#include <tuple>
#include <atomic>
#include <deque>
#include <mutex> 
#include <condition_variable>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        
    private:
        std::vector<std::thread> workers;
        std::atomic<bool> stop_threads;
        
        std::atomic<IRunnable*> current_runnable;
        std::atomic<int> current_num_total_tasks;
        std::atomic<int> next_task_id;
        std::atomic<int> tasks_completed;

        // Synchronization for spinning
        std::atomic<bool> work_available;

        void worker_loop();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */

 struct Task {
    TaskID task_id;
    int count;
    IRunnable* runnable;

    std::atomic<int> outstanding_dependencies;

    std::atomic<int> next_instance_id;
    std::atomic<int> completed_instances;

    Task(TaskID id, int c, IRunnable* r, int num_deps)
        : task_id(id), 
          count(c), 
          runnable(r), 
          outstanding_dependencies(num_deps),
          next_instance_id(0), 
          completed_instances(0) {}
 };

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    
    private:
        std::vector<std::thread> workers;
        std::atomic<bool> stop_threads;
        
        // Synchronization for spinning
        std::condition_variable worker_cv;
        std::condition_variable sync_cv_;
        std::mutex worker_mtx;

        TaskID max_task_id;
        std::atomic<int> total_outstanding_tasks_;

        std::unordered_map<TaskID, Task> queued_tasks;
        std::unordered_map<TaskID, std::vector<TaskID>> task_dependencies;

        std::deque<TaskID> ready_queue_;

        void worker_loop();
};

#endif
