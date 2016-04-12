package rsc.scheduler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import rsc.flow.Cancellation;
import rsc.state.Cancellable;
import rsc.util.UnsignalledExceptions;

/**
 * A TimedScheduler with an embedded, single-threaded ScheduledExecutorService,
 * shared among all workers.
 */
public final class SingleTimedScheduler implements TimedScheduler {

    static final AtomicLong COUNTER = new AtomicLong();
    
    static final ThreadFactory THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "SingleTimedScheduler-" + COUNTER.incrementAndGet());
        return t;
    };

    static final ThreadFactory THREAD_FACTORY_DAEMON = r -> {
        Thread t = new Thread(r, "SingleTimedScheduler-" + COUNTER.incrementAndGet());
        t.setDaemon(true);
        return t;
    };

    final ScheduledThreadPoolExecutor executor;
    
    /**
     * Constructs a new SingleTimedScheduler with non-daemon executor and default
     * naming of "SingleTimedScheduler-N".
     */
    public SingleTimedScheduler() {
        this(THREAD_FACTORY);
    }
    
    /**
     * Constructs a new SingleTimedScheduler with non-daemon executor and
     * named as specified.
     * @param threadName the constant thread name to use
     */
    public SingleTimedScheduler(String threadName) {
        this(r -> {
            Thread t = new Thread(r, threadName);
            return t;
        });
    }

    /**
     * Constructs a new SingleTimedScheduler with possibly daemon executor and default
     * naming of "SingleTimedScheduler-N".
     * @param daemon create a daemon thread?
     */
    public SingleTimedScheduler(boolean daemon) {
        this(daemon ? THREAD_FACTORY_DAEMON : THREAD_FACTORY);
    }
    
    /**
     * Constructs a new SingleTimedScheduler with possible daemon executor and
     * named as specified.
     * @param threadName the constant thread name to use
     * @param daemon create a daemon thread?
     */
    public SingleTimedScheduler(String threadName, boolean daemon) {
        this(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(daemon);
            return t;
        });
    }

    /**
     * Constructs a new SingleTimedScheduler with the given thread factory.
     * @param threadFactory the thread factory to use
     */
    public SingleTimedScheduler(ThreadFactory threadFactory) {
        ScheduledThreadPoolExecutor e = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1, threadFactory);
        e.setRemoveOnCancelPolicy(true);
        executor = e;
    }
    
    @Override
    public Cancellation schedule(Runnable task) {
        try {
            Future<?> f = executor.submit(task);
            return () -> f.cancel(true);
        } catch (RejectedExecutionException ex) {
            return REJECTED;
        }
    }
    
    @Override
    public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
        try {
            Future<?> f = executor.schedule(task, delay, unit);
            return () -> f.cancel(true);
        } catch (RejectedExecutionException ex) {
            return REJECTED;
        }
    }
    
    @Override
    public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        try {
            Future<?> f = executor.scheduleAtFixedRate(task, initialDelay, period, unit);
            return () -> f.cancel(true);
        } catch (RejectedExecutionException ex) {
            return REJECTED;
        }
    }
    
    @Override
    public void start() {
        throw new UnsupportedOperationException("Not supported, yet.");
    }
    
    @Override
    public void shutdown() {
        executor.shutdownNow();
    }
    
    @Override
    public TimedWorker createWorker() {
        return new SingleTimedSchedulerWorker(executor);
    }
    
    static final class SingleTimedSchedulerWorker implements TimedWorker {
        final ScheduledThreadPoolExecutor executor;
        
        OpenHashSet<CancelFuture> tasks;
        
        volatile boolean terminated;
        
        public SingleTimedSchedulerWorker(ScheduledThreadPoolExecutor executor) {
            this.executor = executor;
            this.tasks = new OpenHashSet<>();
        }

        @Override
        public Cancellation schedule(Runnable task) {
            if (terminated) {
                return REJECTED;
            }
            
            TimedScheduledRunnable sr = new TimedScheduledRunnable(task, this);
            
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                
                tasks.add(sr);
            }
            
            try {
                Future<?> f = executor.submit(sr);
                sr.set(f);
            } catch (RejectedExecutionException ex) {
                sr.dispose();
                return REJECTED;
            }
            
            return sr;
        }
        
        void delete(CancelFuture r) {
            synchronized (this) {
                if (!terminated) {
                    tasks.remove(r);
                }
            }
        }
        
        @Override
        public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
            if (terminated) {
                return REJECTED;
            }
            
            TimedScheduledRunnable sr = new TimedScheduledRunnable(task, this);
            
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                
                tasks.add(sr);
            }
            
            try {
                Future<?> f = executor.schedule(sr, delay, unit);
                sr.set(f);
            } catch (RejectedExecutionException ex) {
                sr.dispose();
                return REJECTED;
            }
            
            return sr;
        }
        
        @Override
        public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            if (terminated) {
                return REJECTED;
            }
            
            TimedPeriodicScheduledRunnable sr = new TimedPeriodicScheduledRunnable(task, this);
            
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                
                tasks.add(sr);
            }
            
            try {
                Future<?> f = executor.scheduleAtFixedRate(sr, initialDelay, period, unit);
                sr.set(f);
            } catch (RejectedExecutionException ex) {
                sr.dispose();
                return REJECTED;
            }
            
            return sr;
        }
        
        @Override
        public void shutdown() {
            if (terminated) {
                return;
            }
            terminated = true;
            
            OpenHashSet<CancelFuture> set;
            
            synchronized (this) {
                set = tasks;
                if (set == null) {
                    return;
                }
                tasks = null;
            }
            
            Object[] keys = set.keys;
            for (Object c : keys) {
                if (c != null) {
                    ((CancelFuture)c).cancelFuture();
                }
            }
        }
    }

    interface CancelFuture {
        void cancelFuture();
    }
    
    static final class TimedScheduledRunnable
    extends AtomicReference<Future<?>>
    implements Runnable, Cancellable, Cancellation, CancelFuture {
        /** */
        private static final long serialVersionUID = 2284024836904862408L;
        
        final Runnable task;
        
        final SingleTimedSchedulerWorker parent;
        
        volatile Thread current;
        static final AtomicReferenceFieldUpdater<TimedScheduledRunnable, Thread> CURRENT =
                AtomicReferenceFieldUpdater.newUpdater(TimedScheduledRunnable.class, Thread.class, "current");

        static final Runnable EMPTY = new Runnable() {
            @Override
            public void run() {

            }
        };

        static final Future<?> CANCELLED_FUTURE = new FutureTask<>(EMPTY, null);

        static final Future<?> FINISHED = new FutureTask<>(EMPTY, null);

        public TimedScheduledRunnable(Runnable task, SingleTimedSchedulerWorker parent) {
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
                        if (a != null) {
                            doCancel(a);
                        }
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
        
        @Override
        public void cancelFuture() {
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
        public void dispose() {
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
            return "TimedScheduledRunnable[cancelled=" + get() + ", task=" + task + "]";
        }
    }

    static final class TimedPeriodicScheduledRunnable
    extends AtomicReference<Future<?>>
    implements Runnable, Cancellable, Cancellation, CancelFuture {
        /** */
        private static final long serialVersionUID = 2284024836904862408L;
        
        final Runnable task;
        
        final SingleTimedSchedulerWorker parent;
        
        volatile Thread current;
        static final AtomicReferenceFieldUpdater<TimedPeriodicScheduledRunnable, Thread> CURRENT =
                AtomicReferenceFieldUpdater.newUpdater(TimedPeriodicScheduledRunnable.class, Thread.class, "current");

        static final Runnable EMPTY = new Runnable() {
            @Override
            public void run() {

            }
        };

        static final Future<?> CANCELLED_FUTURE = new FutureTask<>(EMPTY, null);

        static final Future<?> FINISHED = new FutureTask<>(EMPTY, null);

        public TimedPeriodicScheduledRunnable(Runnable task, SingleTimedSchedulerWorker parent) {
            this.task = task;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            CURRENT.lazySet(this, Thread.currentThread());
            try {
                try {
                    task.run();
                } catch (Throwable ex) {
                    UnsignalledExceptions.onErrorDropped(ex);
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
                }
            } finally {
                CURRENT.lazySet(this, null);
            }
        }
        
        void doCancel(Future<?> a) {
            a.cancel(Thread.currentThread() != current);
        }
        
        @Override
        public void cancelFuture() {
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
        public void dispose() {
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
            return "TimedPeriodicScheduledRunnable[cancelled=" + get() + ", task=" + task + "]";
        }
    }

}
