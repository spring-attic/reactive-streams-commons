package reactivestreams.commons.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import reactivestreams.commons.scheduler.Scheduler;
import reactivestreams.commons.state.Cancellable;

/**
 * Wraps a java.util.concurrent.Executor and provides the Scheduler API over it.
 * <p>
 * It supports both non-trampolined worker (for cases where the trampolining happens
 * extenrally) and trampolined worker.
 */
public final class ExecutorScheduler implements Scheduler {

    final Executor executor;
    final boolean trampoline;
    
    public ExecutorScheduler(Executor executor) {
        this(executor, true);
    }

    public ExecutorScheduler(Executor executor, boolean trampoline) {
        this.executor = executor;
        this.trampoline = trampoline;
    }
    
    @Override
    public Cancellable schedule(Runnable task) {
        Objects.requireNonNull(task, "task");
        ExecutorPlainRunnable r = new ExecutorPlainRunnable(task);
        try {
            executor.execute(r);
        } catch (RejectedExecutionException ex) {
            return REJECTED;
        }
        return r;
    }

    @Override
    public Worker createWorker() {
        return trampoline ? new ExecutorSchedulerTrampolineWorker(executor)
                : new ExecutorSchedulerWorker(executor);
    }

    /**
     * A non-tracked runnable that wraps a task and offers cancel support in the form
     * of not executing the task.
     * <p>Since Executor doesn't have cancellation support of its own, the
     * ExecutorRunnable will stay in the Executor's queue and be always executed.
     */
    static final class ExecutorPlainRunnable extends AtomicBoolean 
    implements Runnable, Cancellable {
        /** */
        private static final long serialVersionUID = 5116223460201378097L;
        
        final Runnable task;
        
        public ExecutorPlainRunnable(Runnable task) {
            this.task = task;
        }
        
        @Override
        public void run() {
            try {
                if (!get()) {
                    task.run();
                }
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        @Override
        public void cancel() {
            set(true);
        }
        
        @Override
        public boolean isCancelled() {
            return get();
        }
        
        @Override
        public String toString() {
            return "ExecutorPlainRunnable[cancelled=" + get() + ", task=" + task + "]";
        }
    }
    
    /**
     * Common interface between the tracking workers to signal the need for removal.
     */
    interface WorkerDelete {
        void delete(ExecutorTrackedRunnable r);
    }
    
    /**
     * A Runnable that wraps a task and has reference back to its parent worker to
     * remove itself once completed or cancelled
     */
    static final class ExecutorTrackedRunnable extends AtomicBoolean 
    implements Runnable, Cancellable {
        /** */
        private static final long serialVersionUID = 3503344795919906192L;
        
        final Runnable task;
        final WorkerDelete parent;

        public ExecutorTrackedRunnable(Runnable task, WorkerDelete parent) {
            this.task = task;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            try {
                if (!get()) {
                    task.run();
                }
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                UnsignalledExceptions.onErrorDropped(e);
            } finally {
                cancel();
            }
        }
        
        @Override
        public void cancel() {
            if (compareAndSet(false, true)) {
                parent.delete(this);
            }
        }
        
        @Override
        public boolean isCancelled() {
            return get();
        }
        
        @Override
        public String toString() {
            return "ExecutorTrackedRunnable[cancelled=" + get() + ", task=" + task + "]";
        }
    }
    
    /**
     * A non-trampolining worker that tracks tasks.
     */
    static final class ExecutorSchedulerWorker implements Scheduler.Worker, WorkerDelete {

        final Executor executor;
        
        volatile boolean terminated;
        
        LinkedList<ExecutorTrackedRunnable> tasks;
        
        public ExecutorSchedulerWorker(Executor executor) {
            this.executor = executor;
            this.tasks = new LinkedList<>();
        }

        @Override
        public Cancellable schedule(Runnable task) {
            Objects.requireNonNull(task, "task");
            if (terminated) {
                return REJECTED;
            }
            
            ExecutorTrackedRunnable r = new ExecutorTrackedRunnable(task, this);
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                tasks.add(r);
            }
            
            try {
                executor.execute(r);
            } catch (RejectedExecutionException ex) {
                synchronized (this) {
                    if (!terminated) {
                        tasks.removeLastOccurrence(r);
                    }
                }
                return REJECTED;
            }
            
            return r;
        }

        @Override
        public void shutdown() {
            if (terminated) {
                return;
            }
            LinkedList<ExecutorTrackedRunnable> list;
            synchronized (this) {
                if (terminated) {
                    return;
                }
                terminated = true;
                list = tasks;
                tasks = null;
            }
            
            for (ExecutorTrackedRunnable r : list) {
                r.cancel();
            }
        }
        
        @Override
        public void delete(ExecutorTrackedRunnable r) {
            synchronized (this) {
                if (!terminated) {
                    tasks.remove(r);
                }
            }
        }
        
    }

    /**
     * A trampolining worker that tracks tasks.
     */
    static final class ExecutorSchedulerTrampolineWorker implements Scheduler.Worker, WorkerDelete, Runnable {
        final Executor executor;

        final Queue<ExecutorTrackedRunnable> queue;

        volatile boolean terminated;
        
        volatile int wip;
        static final AtomicIntegerFieldUpdater<ExecutorSchedulerTrampolineWorker> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ExecutorSchedulerTrampolineWorker.class, "wip");
        
        public ExecutorSchedulerTrampolineWorker(Executor executor) {
            this.executor = executor;
            this.queue = new ConcurrentLinkedQueue<>();
        }

        @Override
        public Cancellable schedule(Runnable task) {
            Objects.requireNonNull(task, "task");
            if (terminated) {
                return REJECTED;
            }
            
            ExecutorTrackedRunnable r = new ExecutorTrackedRunnable(task, this);
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                queue.offer(r);
            }
            
            if (WIP.getAndIncrement(this) == 0) {
                try {
                    executor.execute(this);
                } catch (RejectedExecutionException ex) {
                    r.cancel();
                    return REJECTED;
                }
            }
            
            return r;
        }

        @Override
        public void shutdown() {
            if (terminated) {
                return;
            }
            terminated = true;
            final Queue<ExecutorTrackedRunnable> q = queue;
            
            ExecutorTrackedRunnable r;
            
            while ((r = q.poll()) != null && !q.isEmpty()) {
                r.cancel();
            }
        }
        
        @Override
        public void delete(ExecutorTrackedRunnable r) {
            synchronized (this) {
                if (!terminated) {
                    queue.remove(r);
                }
            }
        }
        
        @Override
        public void run() {
            final Queue<ExecutorTrackedRunnable> q = queue;
           
            for (;;) {
                
                int e = 0;
                int r = wip;
                
                while (e != r) {
                    if (terminated) {
                        return;
                    }
                    ExecutorTrackedRunnable task = q.poll();
                    
                    if (task == null) {
                        break;
                    }
                    
                    task.run();
                    
                    e++;
                }
                
                if (e == r && terminated) {
                    return;
                }
                
                if (WIP.addAndGet(this, -e) == 0) {
                    break;
                }
            }
        }
    }

}
