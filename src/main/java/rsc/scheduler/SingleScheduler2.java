package rsc.scheduler;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import rsc.flow.Cancellation;
import rsc.util.ExceptionHelper;
import rsc.util.OpenHashSet;
import rsc.util.UnsignalledExceptions;

/**
 * Scheduler that works with a single-threaded ExecutorService and is suited for
 * same-thread work (like an event dispatch thread).
 */
public final class SingleScheduler2 implements Scheduler {

    static final AtomicLong COUNTER = new AtomicLong();

    static final ThreadFactory THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "parallel-" + COUNTER.incrementAndGet());
        return t;
    };

    static final ThreadFactory THREAD_FACTORY_DAEMON = r -> {
        Thread t = new Thread(r, "parallel-" + COUNTER.incrementAndGet());
        t.setDaemon(true);
        return t;
    };

    final ThreadFactory factory;

    volatile SingleThreadedExecutor executor;
    static final AtomicReferenceFieldUpdater<SingleScheduler2, SingleThreadedExecutor> EXECUTORS =
            AtomicReferenceFieldUpdater.newUpdater(SingleScheduler2.class, SingleThreadedExecutor.class, "executor");

    static final SingleThreadedExecutor TERMINATED;
    static {
        TERMINATED = new SingleThreadedExecutor();
        TERMINATED.shutdown();
    }
    
    public SingleScheduler2() {
        this.factory = THREAD_FACTORY;
        init();
    }

    public SingleScheduler2(String name) {
        this(name, false);
    }

    public SingleScheduler2(String name, boolean daemon) {
        this.factory = r -> {
            Thread t = new Thread(r, name + COUNTER.incrementAndGet());
            t.setDaemon(daemon);
            return t;
        };
        init();
    }

    public SingleScheduler2(ThreadFactory factory) {
        this.factory = factory;
        init();
    }
    
    private void init() {
        EXECUTORS.lazySet(this, new SingleThreadedExecutor());
    }
    
    public boolean isStarted() {
        return executor != TERMINATED;
    }

    @Override
    public void start() {
        SingleThreadedExecutor b = null;
        for (;;) {
            SingleThreadedExecutor a = executor;
            if (a != TERMINATED) {
                if (b != null) {
                    b.shutdown();
                }
                return;
            }

            if (b == null) {
                b = new SingleThreadedExecutor();
            }
            
            if (EXECUTORS.compareAndSet(this, a, b)) {
                return;
            }
        }
    }
    
    @Override
    public void shutdown() {
        SingleThreadedExecutor a = executor;
        if (a != TERMINATED) {
            a = EXECUTORS.getAndSet(this, TERMINATED);
            if (a != TERMINATED) {
                a.shutdown();
            }
        }
    }
    
    @Override
    public Cancellation schedule(Runnable task) {
        try {
            return executor.submit(task);
        } catch (RejectedExecutionException ex) {
            UnsignalledExceptions.onErrorDropped(ex);
            return REJECTED;
        }
    }

    @Override
    public Worker createWorker() {
        return new SingleWorker(executor);
    }
    
    static final class SingleWorker implements Worker {
        final SingleThreadedExecutor exec;
        
        OpenHashSet<SingleWorkerTask> tasks;
        
        volatile boolean shutdown;
        
        public SingleWorker(SingleThreadedExecutor exec) {
            this.exec = exec;
            this.tasks = new OpenHashSet<>();
        }

        @Override
        public Cancellation schedule(Runnable task) {
            if (shutdown) {
                return REJECTED;
            }
            
            SingleWorkerTask pw = new SingleWorkerTask(task, this);
            
            synchronized (this) {
                if (shutdown) {
                    return REJECTED;
                }
                tasks.add(pw);
            }
            
            Cancellation f;
            try {
                f = exec.submit(pw);
            } catch (RejectedExecutionException ex) {
                UnsignalledExceptions.onErrorDropped(ex);
                return REJECTED;
            }
            
            if (shutdown) {
                f.dispose();
                return REJECTED; 
            }
            
            pw.setFuture(f);
            
            return pw;
        }

        @Override
        public void shutdown() {
            if (shutdown) {
                return;
            }
            shutdown = true;
            OpenHashSet<SingleWorkerTask> set;
            synchronized (this) {
                set = tasks;
                tasks = null;
            }
            
            if (set != null && !set.isEmpty()) {
                SingleWorkerTask[] a = set.keys();
                for (SingleWorkerTask o : a) {
                    if (o != null) {
                        o.cancelFuture();
                    }
                }
            }
        }
        
        void remove(SingleWorkerTask task) {
            if (shutdown) {
                return;
            }
            
            synchronized (this) {
                if (shutdown) {
                    return;
                }
                tasks.remove(task);
            }
        }
        
        int pendingTasks() {
            if (shutdown) {
                return 0;
            }
            
            synchronized (this) {
                OpenHashSet<?> set = tasks;
                if (set != null) {
                    return set.size();
                }
                return 0;
            }
        }
        
        static final class SingleWorkerTask implements Runnable, Cancellation {
            final Runnable run;
            
            final SingleWorker parent;
            
            volatile boolean cancelled;
            
            volatile Cancellation future;
            static final AtomicReferenceFieldUpdater<SingleWorkerTask, Cancellation> FUTURE =
                    AtomicReferenceFieldUpdater.newUpdater(SingleWorkerTask.class, Cancellation.class, "future");
            
            static final Cancellation FINISHED = () -> { };
            static final Cancellation CANCELLED = () -> { };
            
            public SingleWorkerTask(Runnable run, SingleWorker parent) {
                this.run = run;
                this.parent = parent;
            }
            
            @Override
            public void run() {
                if (cancelled || parent.shutdown) {
                    return;
                }
                try {
                    try {
                        run.run();
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                } finally {
                    for (;;) {
                        Cancellation f = future;
                        if (f == CANCELLED) {
                            break;
                        }
                        if (FUTURE.compareAndSet(this, f, FINISHED)) {
                            parent.remove(this);
                            break;
                        }
                    }
                }
                return;
            }
            
            @Override
            public void dispose() {
                if (!cancelled) {
                    cancelled = true;
                    
                    Cancellation f = future;
                    if (f != CANCELLED && f != FINISHED) {
                        f = FUTURE.getAndSet(this, CANCELLED);
                        if (f != CANCELLED && f != FINISHED) {
                            if (f != null) {
                                f.dispose();
                            }
                            
                            parent.remove(this);
                        }
                    }
                }
            }
            
            void setFuture(Cancellation f) {
                if (future != null || !FUTURE.compareAndSet(this, null, f)) {
                    if (future != FINISHED) {
                        f.dispose();
                    }
                }
            }
            
            void cancelFuture() {
                Cancellation f = future;
                if (f != CANCELLED && f != FINISHED) {
                    f = FUTURE.getAndSet(this, CANCELLED);
                    if (f != null && f != CANCELLED && f != FINISHED) {
                        f.dispose();
                    }
                }
            }
        }
    }
}
