package rsc.scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import rsc.flow.Cancellation;

public final class SingleThreadedExecutor implements Runnable {

    final BlockingQueue<Runnable> queue;
    
    volatile boolean shutdown;
    
    final AtomicBoolean once;
    
    volatile Thread thread;
    
    static final int spinLimit = 64;
    
    public SingleThreadedExecutor() {
        this.queue = new LinkedBlockingQueue<>();
        this.once = new AtomicBoolean();
    }
    
    @Override
    public void run() {
        BlockingQueue<Runnable> q = queue;
        
        for (;;) {
            
            if (shutdown) {
                return;
            }
            
            int spin = spinLimit;
            
            while (--spin != 0 && q.isEmpty()) {
                if (shutdown) {
                    return;
                }
            }
            

            
            try {
                Runnable run = q.poll(1, TimeUnit.MILLISECONDS);

                if (shutdown) {
                    return;
                }
                
                if (run != null) {
                    try {
                        run.run();
                    } catch (Throwable ex) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, ex);
                    } finally {
                        if (Thread.currentThread().isInterrupted()) {
                            Thread.interrupted();
                        }
                    }
                }
            } catch (InterruptedException e) {
                // ignore for now
            }
        }
    }
    
    public Cancellation submit(Runnable task) {
        if (shutdown) {
            throw new RejectedExecutionException();
        }
        if (!once.get() && once.compareAndSet(false, true)) {
            Thread th = new Thread(this);
            thread = th;
            th.start();
        }
        
        Task t = new Task(task, this);
        
        queue.offer(t);

        if (shutdown) {
            queue.clear();
            thread = null;
            throw new RejectedExecutionException();
        }

        return t;
    }
    
    public void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        thread = null;
        queue.clear();
    }
    
    static final class Task implements Runnable, Cancellation {
        
        final Runnable task;
        
        final SingleThreadedExecutor parent;

        volatile boolean stop;
        
        public Task(Runnable task, SingleThreadedExecutor parent) {
            this.task = task;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            if (stop || parent.shutdown) {
                return;
            }
            
            task.run();
        }
        
        @Override
        public void dispose() {
            if (!stop) {
                stop = true;
            }
        }
    }
}
