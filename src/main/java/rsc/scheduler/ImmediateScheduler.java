package rsc.scheduler;

import rsc.flow.Disposable;
import rsc.util.*;

/**
 * Executes tasks on the caller's thread immediately.
 * <p>
 * Use the ImmediateScheduler.instance() to get a shared, stateless instance of this scheduler.
 */
public final class ImmediateScheduler implements Scheduler {

    private static final ImmediateScheduler INSTANCE = new ImmediateScheduler();
    
    public static Scheduler instance() {
        return INSTANCE;
    }
    
    private ImmediateScheduler() {
        
    }
    
    static final Disposable EMPTY = () -> { };
    
    @Override
    public Disposable schedule(Runnable task) {
        try {
            task.run();
        } catch (Throwable ex) {
            ExceptionHelper.throwIfFatal(ex);
            UnsignalledExceptions.onErrorDropped(ex);
        }
        return EMPTY;
    }

    @Override
    public Worker createWorker() {
        return new ImmediateSchedulerWorker();
    }
    
    static final class ImmediateSchedulerWorker implements Scheduler.Worker {
        
        volatile boolean shutdown;

        @Override
        public Disposable schedule(Runnable task) {
            if (shutdown) {
                return REJECTED;
            }
            try {
                task.run();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                UnsignalledExceptions.onErrorDropped(ex);
            }
            return EMPTY;
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }
    }

}
