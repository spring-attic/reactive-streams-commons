package rsc.scheduler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import rsc.flow.Cancellation;
import rsc.util.*;

/**
 * Scheduler that works with a single-threaded ExecutorService and is suited for
 * same-thread work (like an event dispatch thread).
 */
public final class SingleScheduler implements Scheduler {

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

    volatile ExecutorService executor;
    static final AtomicReferenceFieldUpdater<SingleScheduler, ExecutorService> EXECUTORS =
            AtomicReferenceFieldUpdater.newUpdater(SingleScheduler.class, ExecutorService.class, "executor");

    static final ExecutorService TERMINATED;
    static {
        TERMINATED = Executors.newSingleThreadExecutor();
        TERMINATED.shutdownNow();
    }
    
    public SingleScheduler() {
        this.factory = THREAD_FACTORY;
        init();
    }

    public SingleScheduler(String name) {
        this(name, false);
    }

    public SingleScheduler(String name, boolean daemon) {
        this.factory = r -> {
            Thread t = new Thread(r, name + COUNTER.incrementAndGet());
            t.setDaemon(daemon);
            return t;
        };
        init();
    }

    public SingleScheduler(ThreadFactory factory) {
        this.factory = factory;
        init();
    }
    
    private void init() {
        EXECUTORS.lazySet(this, Executors.newSingleThreadExecutor(factory));
    }
    
    public boolean isStarted() {
        return executor != TERMINATED;
    }

    @Override
    public void start() {
        ExecutorService b = null;
        for (;;) {
            ExecutorService a = executor;
            if (a != TERMINATED) {
                if (b != null) {
                    b.shutdownNow();
                }
                return;
            }

            if (b == null) {
                b = Executors.newSingleThreadExecutor(factory);
            }
            
            if (EXECUTORS.compareAndSet(this, a, b)) {
                return;
            }
        }
    }
    
    @Override
    public void shutdown() {
        ExecutorService a = executor;
        if (a != TERMINATED) {
            a = EXECUTORS.getAndSet(this, TERMINATED);
            if (a != TERMINATED) {
                a.shutdownNow();
            }
        }
    }
    
    @Override
    public Cancellation schedule(Runnable task) {
        try {
            Future<?> f = executor.submit(task);
            return () -> f.cancel(true);
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
        final ExecutorService exec;
        
        OpenHashSet<SingleWorkerTask> tasks;
        
        volatile boolean shutdown;
        
        public SingleWorker(ExecutorService exec) {
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
            
            Future<?> f;
            try {
                f = exec.submit(pw);
            } catch (RejectedExecutionException ex) {
                UnsignalledExceptions.onErrorDropped(ex);
                return REJECTED;
            }
            
            if (shutdown) {
                f.cancel(true);
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
                Object[] a = set.keys;
                for (Object o : a) {
                    if (o != null) {
                        ((SingleWorkerTask)o).cancelFuture();
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
            
            volatile Future<?> future;
            @SuppressWarnings("rawtypes")
            static final AtomicReferenceFieldUpdater<SingleWorkerTask, Future> FUTURE =
                    AtomicReferenceFieldUpdater.newUpdater(SingleWorkerTask.class, Future.class, "future");
            
            static final Future<Object> FINISHED = CompletableFuture.completedFuture(null);
            static final Future<Object> CANCELLED = CompletableFuture.completedFuture(null);
            
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
                        Future<?> f = future;
                        if (f == CANCELLED) {
                            break;
                        }
                        if (FUTURE.compareAndSet(this, f, FINISHED)) {
                            parent.remove(this);
                            break;
                        }
                    }
                }
            }
            
            @Override
            public void dispose() {
                if (!cancelled) {
                    cancelled = true;
                    
                    Future<?> f = future;
                    if (f != CANCELLED && f != FINISHED) {
                        f = FUTURE.getAndSet(this, CANCELLED);
                        if (f != CANCELLED && f != FINISHED) {
                            if (f != null) {
                                f.cancel(true);
                            }
                            
                            parent.remove(this);
                        }
                    }
                }
            }
            
            void setFuture(Future<?> f) {
                if (future != null || !FUTURE.compareAndSet(this, null, f)) {
                    if (future != FINISHED) {
                        f.cancel(true);
                    }
                }
            }
            
            void cancelFuture() {
                Future<?> f = future;
                if (f != CANCELLED && f != FINISHED) {
                    f = FUTURE.getAndSet(this, CANCELLED);
                    if (f != null && f != CANCELLED && f != FINISHED) {
                        f.cancel(true);
                    }
                }
            }
        }
    }
}
