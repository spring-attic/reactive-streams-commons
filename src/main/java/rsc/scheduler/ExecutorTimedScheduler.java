package rsc.scheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import rsc.flow.Cancellation;
import rsc.util.*;

/**
 * Wraps an existing ScheduledExecutorService and provides TimedScheduler services over it,
 * sharing the executor among workers.
 */
public final class ExecutorTimedScheduler implements TimedScheduler {

    final ScheduledExecutorService executor;
    
    public ExecutorTimedScheduler(ScheduledExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public Cancellation schedule(Runnable task) {
        Future<?> f = executor.submit(task);
        return () -> f.cancel(true);
    }

    @Override
    public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
        Future<?> f = executor.schedule(task, delay, unit);
        return () -> f.cancel(true);
    }

    @Override
    public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        Future<?> f = executor.scheduleAtFixedRate(task, initialDelay, period, unit);
        return () -> f.cancel(true);
    }

    @Override
    public TimedWorker createWorker() {
        return new ExecutorTimedWorker(executor);
    }
    
    static final class ExecutorTimedWorker implements TimedWorker, Runnable {
        final ScheduledExecutorService executor;
        
        final SpscLinkedArrayQueue<Runnable> queue;
        
        volatile boolean stopped;
        
        OpenHashSet<AbstractTimedTask> tasks;
        
        volatile int wip;
        static final AtomicIntegerFieldUpdater<ExecutorTimedWorker> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ExecutorTimedWorker.class, "wip");
        
        public ExecutorTimedWorker(ScheduledExecutorService executor) {
            this.executor = executor;
            this.queue = new SpscLinkedArrayQueue<>(16);
            this.tasks = new OpenHashSet<>();
        }

        @Override
        public Cancellation schedule(Runnable task) {
            if (stopped) {
                return REJECTED;
            }
            
            Queue<Runnable> queue = this.queue;
            
            ScheduledTask st = new ScheduledTask(task, this);
            synchronized (this) {
                queue.offer(st);
            }
            if (WIP.getAndIncrement(this) == 0) {
                if (stopped) {
                    queue.clear();
                    return REJECTED;
                } else {
                    executor.execute(this);
                }
            }
            return st;
        }

        @Override
        public void shutdown() {
            if (!stopped) {
                stopped = true;
                
                if (WIP.getAndIncrement(this) == 0) {
                    queue.clear();
                }
                
                OpenHashSet<AbstractTimedTask> set;
                synchronized (this) {
                    set = tasks;
                    tasks = null;
                }
                
                if (set != null) {
                    Object[] keys = set.keys;
                    for (Object o : keys) {
                        if (o != null) {
                            ((AbstractTimedTask)o).cancelFuture();
                        }
                    }
                }
            }
        }
        
        void remove(AbstractTimedTask t) {
            if (stopped) {
                return;
            }
            synchronized (this) {
                if (stopped) {
                    return;
                }
                
                tasks.remove(t);
            }
        }

        @Override
        public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
            if (stopped) {
                return REJECTED;
            }
            
            TimedScheduledTask t = new TimedScheduledTask(task, this);
            
            synchronized (this) {
                if (stopped) {
                    return REJECTED;
                }
                
                tasks.add(t);
            }
            
            Future<?> f = executor.schedule(t, delay, unit);
            
            if (stopped) {
                f.cancel(true);
                return REJECTED;
            }
            
            t.setFuture(f);
            
            return t;
        }

        @Override
        public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            if (stopped) {
                return REJECTED;
            }
            
            TimedPeriodicTask t = new TimedPeriodicTask(task, this);
            
            synchronized (this) {
                if (stopped) {
                    return REJECTED;
                }
                
                tasks.add(t);
            }
            
            Future<?> f = executor.scheduleAtFixedRate(t, initialDelay, period, unit);
            
            if (stopped) {
                f.cancel(true);
                return REJECTED;
            }
            
            t.setFuture(f);
            
            return t;
        }
        
        void scheduleRecursive(Runnable run) {
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                run.run();
                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }
            } else {
                synchronized (this) {
                    queue.offer(run);
                }
                
                if (WIP.getAndIncrement(this) != 0) {
                    return;
                }
            }
            run();
        }
        
        @Override
        public void run() {
            Queue<Runnable> queue = this.queue;
            do {
                if (stopped) {
                    queue.clear();
                } else {
                    Runnable r = queue.poll();
                    if (stopped) {
                        queue.clear();
                    } else {
                        r.run();
                    }
                }
            } while (WIP.decrementAndGet(this) != 0);
        }
        
        static final class ScheduledTask implements Runnable, Cancellation {
            final Runnable run;
            
            final ExecutorTimedWorker parent;
            
            volatile boolean cancelled;
            
            public ScheduledTask(Runnable run, ExecutorTimedWorker parent) {
                this.run = run;
                this.parent = parent;
            }
            
            @Override
            public void run() {
                if (!cancelled && !parent.stopped) {
                    try {
                        run.run();
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                }
            }
            
            @Override
            public void dispose() {
                cancelled = true;
            }
        }
        
        static abstract class AbstractTimedTask implements Cancellation, Runnable {
            final ExecutorTimedWorker parent;
            
            volatile boolean cancelled;
            
            volatile Future<?> future;
            @SuppressWarnings("rawtypes")
            static final AtomicReferenceFieldUpdater<AbstractTimedTask, Future> FUTURE =
                    AtomicReferenceFieldUpdater.newUpdater(AbstractTimedTask.class, Future.class, "future");
            
            static final FutureTask<Object> FINISHED = new FutureTask<>(() -> { }, null);

            static final FutureTask<Object> CANCELLED = new FutureTask<>(() -> { }, null);

            public AbstractTimedTask(ExecutorTimedWorker parent) {
                this.parent = parent;
            }
            
            @Override
            public final void dispose() {
                if (!cancelled) {
                    cancelled = true;
                    cancelFuture();
                    parent.remove(this);
                }
            }
            
            final void setFuture(Future<?> f) {
                for (;;) {
                    Future<?> a = future;
                    if (a == FINISHED) {
                        return;
                    }
                    if (a != null) {
                        f.cancel(true);
                        return;
                    }
                    if (FUTURE.compareAndSet(this, null, f)) {
                        return;
                    }
                }
            }
            
            final void cancelFuture() {
                Future<?> f = future;
                if (f != CANCELLED && f != FINISHED) {
                    f = FUTURE.getAndSet(this, CANCELLED);
                    if (f != null && f != CANCELLED && f != FINISHED) {
                        f.cancel(true);
                    }
                }
            }
        }
        
        static final class TimedScheduledTask extends AbstractTimedTask {
            final Runnable run;
            
            boolean once;
            
            public TimedScheduledTask(Runnable run, ExecutorTimedWorker parent) {
                super(parent);
                this.run = run;
            }
            
            @Override
            public void run() {
                if (cancelled || parent.stopped) {
                    return;
                }
                if (!once) {
                    once = true;
                    
                    FUTURE.lazySet(this, FINISHED);
                    parent.remove(this);
                    
                    parent.scheduleRecursive(this);
                } else {
                    try {
                        run.run();
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                }
            }
            
        }
        
        static final class TimedPeriodicTask extends AbstractTimedTask {
            final InnerRunner run;
            
            public TimedPeriodicTask(Runnable task, ExecutorTimedWorker parent) {
                super(parent);
                this.run = new InnerRunner(task);
            }
            
            @Override
            public void run() {
                if (cancelled || parent.stopped) {
                    return;
                }
                
                parent.scheduleRecursive(run);
            }
            
            final class InnerRunner implements Runnable {
                final Runnable run;
                
                public InnerRunner(Runnable run) {
                    this.run = run;
                }
                @Override
                public void run() {
                    try {
                        run.run();
                    } catch (Throwable ex) {
                        dispose();
                        ExceptionHelper.throwIfFatal(ex);
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                }
            }
        }
    }
}
