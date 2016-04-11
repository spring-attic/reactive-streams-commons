package reactivestreams.commons.util;

import java.util.concurrent.Executor;

import reactivestreams.commons.scheduler.Scheduler;
import reactivestreams.commons.util.ExecutorScheduler.ExecutorSchedulerWorker;

/**
 * Wraps one of the workers of some other Scheduler and
 * provides Worker services on top of it.
 * <p>
 * Use the shutdown() to release the wrapped worker.
 */
public final class WorkerScheduler implements Scheduler, Executor {

    final Worker main;
    
    public WorkerScheduler(Scheduler actual) {
        this.main = actual.createWorker();
    }
    
    @Override
    public void shutdown() {
        main.shutdown();
    }
    
    @Override
    public Runnable schedule(Runnable task) {
        return main.schedule(task);
    }
    
    @Override
    public void execute(Runnable command) {
        main.schedule(command);
    }
    
    @Override
    public Worker createWorker() {
        return new ExecutorSchedulerWorker(this);
    }
    
}
