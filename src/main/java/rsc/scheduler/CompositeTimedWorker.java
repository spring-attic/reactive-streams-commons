package rsc.scheduler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import rsc.flow.Disposable;
import rsc.scheduler.TimedScheduler.TimedWorker;
import rsc.util.OpenHashSet;

/**
 * Wraps another TimedWorker and tracks Runnable tasks scheduled with it.
 */
public final class CompositeTimedWorker implements TimedWorker {
    
    final TimedWorker actual;
    
    OpenHashSet<TimedTask> tasks;
    
    volatile boolean terminated;
    
    public CompositeTimedWorker(TimedWorker actual) {
        this.actual = actual;
        this.tasks = new OpenHashSet<>();
    }
    
    @Override
    public Disposable schedule(Runnable task) {
        if (terminated) {
            return Scheduler.REJECTED;
        }
        
        SingleTask st = new SingleTask(task, this);
        
        synchronized (this) {
            if (terminated) {
                return Scheduler.REJECTED;
            }
            tasks.add(st);
        }
        
        Disposable f;
        
        try {
            f = actual.schedule(st);
        } catch (final Throwable ex) {
            delete(st);
            throw ex;
        }
        
        
        st.setFuture(f);
        
        return st;
    }
    
    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        if (terminated) {
            return Scheduler.REJECTED;
        }
        
        SingleTask st = new SingleTask(task, this);
        
        synchronized (this) {
            if (terminated) {
                return Scheduler.REJECTED;
            }
            tasks.add(st);
        }
        
        Disposable f;
        
        try {
            f = actual.schedule(st, delay, unit);
        } catch (final Throwable ex) {
            delete(st);
            throw ex;
        }

        st.setFuture(f);
        
        return st;
    }
    
    @Override
    public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        if (terminated) {
            return Scheduler.REJECTED;
        }
        
        PeriodicTask st = new PeriodicTask(task, this);
        
        synchronized (this) {
            if (terminated) {
                return Scheduler.REJECTED;
            }
            tasks.add(st);
        }
        
        Disposable f;
        
        try {
            f = actual.schedulePeriodically(st, initialDelay, period, unit);
        } catch (final Throwable ex) {
            delete(st);
            throw ex;
        }

        st.setFuture(f);
        
        return st;
    }
    
    @Override
    public void shutdown() {
        if (terminated) {
            return;
        }
        terminated = true;
        OpenHashSet<TimedTask> set;
        synchronized (this) {
            set = tasks;
            tasks = null;
        }
        
        if (set != null) {
            Object[] array = set.keys();
            for (Object tt : array) {
                if (tt != null) {
                    ((TimedTask)tt).cancelFuture();
                }
            }
        }
    }
    
    void delete(TimedTask f) {
        if (terminated) {
            return;
        }
        
        synchronized (this) {
            if (terminated) {
                return;
            }
            tasks.remove(f);
        }
    }
    
    static final Disposable FINISHED  = () -> { };
    static final Disposable CANCELLED = () -> { };

    static abstract class TimedTask implements Runnable, Disposable {
        final CompositeTimedWorker parent;
        
        final Runnable run;
        
        volatile Disposable future;
        static final AtomicReferenceFieldUpdater<CompositeTimedWorker.TimedTask, Disposable> FUTURE =
                AtomicReferenceFieldUpdater.newUpdater(CompositeTimedWorker.TimedTask.class, Disposable.class, "future");
        
        public TimedTask(Runnable run, CompositeTimedWorker parent) {
            this.run = run;
            this.parent = parent;
        }
        
        final void setFuture(Disposable f) {
            for (;;) {
                Disposable c = future;
                if (c == FINISHED) {
                    break;
                }
                if (c == CANCELLED) {
                    f.dispose();
                    break;
                }
                if (FUTURE.compareAndSet(this, null, f)) {
                    break;
                }
            }
        }
        
        final void cancelFuture() {
            for (;;) {
                Disposable c = future;
                if (c == FINISHED || c == CANCELLED) {
                    break;
                }
                if (FUTURE.compareAndSet(this, c, CANCELLED)) {
                    if (c != null) {
                        c.dispose();
                    }
                    break;
                }
            }
        }
        
        @Override
        public final void dispose() {
            for (;;) {
                Disposable c = future;
                if (c == FINISHED || c == CANCELLED) {
                    break;
                }
                if (FUTURE.compareAndSet(this, c, CANCELLED)) {
                    parent.delete(this);
                    if (c != null) {
                        c.dispose();
                    }
                    break;
                }
            }
        }
    }
    
    static final class SingleTask extends CompositeTimedWorker.TimedTask {

        public SingleTask(Runnable run, CompositeTimedWorker parent) {
            super(run, parent);
        }
        
        @Override
        public void run() {
            try {
                run.run();
            } finally {
                for (;;) {
                    Disposable c = future;
                    if (c == CANCELLED) {
                        break;
                    }
                    if (FUTURE.compareAndSet(this, c, FINISHED)) {
                        parent.delete(this);
                        break;
                    }
                }
            }
        }
    }
    static final class PeriodicTask extends CompositeTimedWorker.TimedTask {

        public PeriodicTask(Runnable run, CompositeTimedWorker parent) {
            super(run, parent);
        }

        @Override
        public void run() {
            if (future == CANCELLED) {
                return;
            }
            try {
                run.run();
            } catch (final Throwable ex) {
                for (;;) {
                    Disposable c = future;
                    if (c == CANCELLED) {
                        break;
                    }
                    if (FUTURE.compareAndSet(this, c, FINISHED)) {
                        parent.delete(this);
                        break;
                    }
                }
                
                throw ex;
            }
        }
    }
}