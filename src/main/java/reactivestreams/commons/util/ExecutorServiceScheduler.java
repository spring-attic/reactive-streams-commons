package reactivestreams.commons.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * An Rsc scheduler which uses a backing ExecutorService to schedule Runnables for async operators. 
 */
public final class ExecutorServiceScheduler implements Callable<Function<Runnable, Runnable>> {

    static final Runnable EMPTY = new Runnable() {
        @Override
        public void run() {

        }
    };

    static final Future<?> CANCELLED = new FutureTask<>(EMPTY, null);

    static final Future<?> FINISHED = new FutureTask<>(EMPTY, null);

    final ExecutorService executor;

    public ExecutorServiceScheduler(ExecutorService executor) {
        this.executor = executor;
    }
    
    @Override
    public Function<Runnable, Runnable> call() throws Exception {
        return new ExecutorServiceWorker(executor);
    }

    static final class ExecutorServiceWorker implements Function<Runnable, Runnable> {
        
        final ExecutorService executor;
        
        volatile boolean terminated;
        
        Collection<ScheduledRunnable> tasks;
        
        public ExecutorServiceWorker(ExecutorService executor) {
            this.executor = executor;
            this.tasks = new LinkedList<>();
        }
        
        @Override
        public Runnable apply(Runnable t) {
            if (t == null) {
                terminate();
                return EMPTY;
            }
            
            ScheduledRunnable sr = new ScheduledRunnable(t, this);
            if (add(sr)) {
                Future<?> f = executor.submit(sr::runTask);
                sr.setFuture(f);
                return sr;
            }
            
            return EMPTY;
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
        
        void terminate() {
            if (!terminated) {
                Collection<ScheduledRunnable> coll;
                synchronized (this) {
                    if (terminated) {
                        return;
                    }
                    coll = tasks;
                    tasks = null;
                }
                for (ScheduledRunnable sr : coll) {
                    sr.cancelFuture();
                }
            }
        }
    }
    
    static final class ScheduledRunnable
    extends AtomicReference<Future<?>>
    implements Runnable {
        /** */
        private static final long serialVersionUID = 2284024836904862408L;
        
        final Runnable task;
        
        final ExecutorServiceWorker parent;

        public ScheduledRunnable(Runnable task, ExecutorServiceWorker parent) {
            this.task = task;;
            this.parent = parent;
        }
        
        public void runTask() {
            try {
                try {
                    task.run();
                } catch (Throwable e) {
                    UnsignalledExceptions.onErrorDropped(e);
                }
            } finally {
                for (;;) {
                    Future<?> a = get();
                    if (a == CANCELLED) {
                        break;
                    }
                    if (compareAndSet(a, FINISHED)) {
                        parent.delete(this);
                        break;
                    }
                }
            }
        }
        
        @Override
        public void run() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED)) {
                    if (a != null) {
                        a.cancel(true);
                    }
                    parent.delete(this);
                    return;
                }
            }
        }

        void cancelFuture() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED)) {
                    if (a != null) {
                        a.cancel(true);
                    }
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
                if (a == CANCELLED) {
                    f.cancel(true);
                    return;
                }
                if (compareAndSet(null, f)) {
                    return;
                }
            }
        }
    }

}
