package reactivestreams.commons.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import reactivestreams.commons.scheduler.Scheduler;
import reactivestreams.commons.state.Cancellable;
import reactivestreams.commons.util.ExecutorScheduler.ExecutorSchedulerTrampolineWorker;

/**
 * An Rsc scheduler which uses a backing ExecutorService to schedule Runnables for async operators. 
 */
public final class ExecutorServiceScheduler implements Scheduler {

    static final Runnable EMPTY = new Runnable() {
        @Override
        public void run() {

        }
    };

    static final Future<?> CANCELLED_FUTURE = new FutureTask<>(EMPTY, null);

    static final Future<?> FINISHED = new FutureTask<>(EMPTY, null);

    final ExecutorService executor;

    final boolean trampoline;

    public ExecutorServiceScheduler(ExecutorService executor) {
        this(executor, true);
    }

    public ExecutorServiceScheduler(ExecutorService executor, boolean trampoline) {
        this.executor = executor;
        this.trampoline = trampoline;
    }
    
    @Override
    public Worker createWorker() {
        if  (trampoline) {
            return new ExecutorSchedulerTrampolineWorker(executor);
        }
        return new ExecutorServiceWorker(executor);
    }
    
    @Override
    public Cancellable schedule(Runnable task) {
        Future<?> f = executor.submit(task);
        return new CancellableFuture(f);
    }

    static final class ExecutorServiceWorker implements Worker {
        
        final ExecutorService executor;
        
        volatile boolean terminated;
        
        Collection<ScheduledRunnable> tasks;
        
        public ExecutorServiceWorker(ExecutorService executor) {
            this.executor = executor;
            this.tasks = new LinkedList<>();
        }
        
        @Override
        public Cancellable schedule(Runnable t) {
            ScheduledRunnable sr = new ScheduledRunnable(t, this);
            if (add(sr)) {
                Future<?> f = executor.submit(sr);
                sr.setFuture(f);
            }
            return sr;
        }
        
        boolean add(ScheduledRunnable sr) {
            if (!terminated) {
                synchronized (this) {
                    if (!terminated) {
                        tasks.add(sr);
                        return true;
                    }
                }
            }
            return false;
        }
        
        void delete(ScheduledRunnable sr) {
            if (!terminated) {
                synchronized (this) {
                    if (!terminated) {
                        tasks.remove(sr);
                    }
                }
            }
        }
        
        @Override
        public void shutdown() {
            if (!terminated) {
                Collection<ScheduledRunnable> coll;
                synchronized (this) {
                    if (terminated) {
                        return;
                    }
                    coll = tasks;
                    tasks = null;
                    terminated = true;
                }
                for (ScheduledRunnable sr : coll) {
                    sr.cancelFuture();
                }
            }
        }
    }
    
    static final class ScheduledRunnable
    extends AtomicReference<Future<?>>
    implements Runnable, Cancellable {
        /** */
        private static final long serialVersionUID = 2284024836904862408L;
        
        final Runnable task;
        
        final ExecutorServiceWorker parent;
        
        volatile Thread current;
        static final AtomicReferenceFieldUpdater<ScheduledRunnable, Thread> CURRENT =
                AtomicReferenceFieldUpdater.newUpdater(ScheduledRunnable.class, Thread.class, "current");

        public ScheduledRunnable(Runnable task, ExecutorServiceWorker parent) {
            this.task = task;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            CURRENT.lazySet(this, Thread.currentThread());
            try {
                try {
                    task.run();
                } catch (Throwable e) {
                    UnsignalledExceptions.onErrorDropped(e);
                }
            } finally {
                for (;;) {
                    Future<?> a = get();
                    if (a == CANCELLED_FUTURE) {
                        break;
                    }
                    if (compareAndSet(a, FINISHED)) {
                        parent.delete(this);
                        break;
                    }
                }
                CURRENT.lazySet(this, null);
            }
        }
        
        void doCancel(Future<?> a) {
            a.cancel(Thread.currentThread() != current);
        }
        
        void cancelFuture() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED_FUTURE)) {
                    if (a != null) {
                        doCancel(a);
                    }
                    return;
                }
            }
        }
        
        @Override
        public boolean isCancelled() {
            Future<?> f = get();
            return f == FINISHED || f == CANCELLED_FUTURE;
        }
        
        @Override
        public void cancel() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED_FUTURE)) {
                    if (a != null) {
                        doCancel(a);
                    }
                    parent.delete(this);
                    return;
                }
            }
        }

        
        void setFuture(Future<?> f) {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (a == CANCELLED_FUTURE) {
                    doCancel(a);
                    return;
                }
                if (compareAndSet(null, f)) {
                    return;
                }
            }
        }
        
        @Override
        public String toString() {
            return "ScheduledRunnable[cancelled=" + get() + ", task=" + task + "]";
        }
    }
}
