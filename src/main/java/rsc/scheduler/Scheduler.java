package rsc.scheduler;

import rsc.flow.Disposable;

/**
 * Provides an abstract asychronous boundary to operators.
 */
public interface Scheduler {
    /**
     * Schedules the given task on this scheduler non-delayed execution.
     * 
     * <p>
     * This method is safe to be called from multiple threads but there are no
     * ordering guarantees between tasks.
     * 
     * @param task the task to execute
     * 
     * @return the Disposable instance that let's one cancel this particular task.
     * If the Scheduler has been shut down, the {@link #REJECTED} Disposable instance is returned.
     */
    Disposable schedule(Runnable task);
    
    /**
     * Creates a worker of this Scheduler that executed task in a strict
     * FIFO order, guaranteed non-concurrently with each other.
     * <p>
     * Once the Worker is no longer in use, one should call shutdown() on it to
     * release any resources the particular Scheduler may have used.
     * 
     * <p>Tasks scheduled with this worker run in FIFO order and strictly non-concurrently, but
     * there are no ordering guarantees between different Workers created from the same
     * Scheduler.
     * 
     * @return the Worker instance.
     */
    Worker createWorker();
    
    /**
     * Instructs this Scheduler to prepare itself for running tasks
     * directly or through its Workers.
     * 
     * <p>The operation is thread-safe but one should avoid using
     * start() and shutdown() concurrently as it would non-deterministically
     * leave the Scheduler in either active or inactive state.
     */
    default void start() {
        
    }
    
    /**
     * Instructs this Scheduler to release all resources and reject
     * any new tasks to be executed.
     * 
     * <p>The operation is thread-safe but one should avoid using
     * start() and shutdown() concurrently as it would non-deterministically
     * leave the Scheduler in either active or inactive state.
     */
    default void shutdown() {
        
    }
    
    /**
     * A worker representing an asynchronous boundary that executes tasks in
     * a FIFO order, guaranteed non-concurrently with respect to each other.
     * 
     * <p>Implementors note:<br>
     * The shutdown() method should be implemented in a way that shutting down a
     * worker of a Scheduler doesn't shuts down other Workers from the same
     * Scheduler.
     */
    interface Worker {
        
        /**
         * Schedules the task on this worker.
         * @param task the task to schedule
         * @return the Disposable instance that let's one cancel this particular task.
         * If the Scheduler has been shut down, the {@link #REJECTED} Disposable instance is returned.
         */
        Disposable schedule(Runnable task);
        
        /**
         * Instructs this worker to cancel all pending tasks, all running tasks in 
         * a best-effort manner, reject new tasks and
         * release any resources associated with it.
         */
        void shutdown();
    }
    
    /**
     * Returned by the schedule() methods if the Scheduler or the Worker has ben shut down.
     */
    Disposable REJECTED = new Disposable() {
        @Override
        public void dispose() {
            // deliberately no-op
        }
        
        @Override
        public String toString() {
            return "Rejected task";
        }
    };
}
