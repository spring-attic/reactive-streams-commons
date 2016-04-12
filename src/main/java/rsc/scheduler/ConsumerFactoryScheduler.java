package rsc.scheduler;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import rsc.flow.Cancellation;
import rsc.state.Cancellable;
import rsc.util.ExceptionHelper;

/**
 * Wraps the old Callable&lt;Consumer&lt;Runnable>> based API into a scheduler.
 */
public final class ConsumerFactoryScheduler implements Scheduler {
    
    final Callable<? extends Consumer<Runnable>> factory;
    
    public ConsumerFactoryScheduler(Callable<? extends Consumer<Runnable>> factory) {
        this.factory = factory;
    }
    
    @Override
    public Cancellation schedule(Runnable task) {
        Objects.requireNonNull(task, "task");
        
        ConsumerFactoryWorker w; 
        
        try {
            w = new ConsumerFactoryWorker(factory.call());
        } catch (Exception ex) {
            ExceptionHelper.failUpstream(ex);
            return REJECTED;
        }
        
        w.schedule(() -> {
            try {
                task.run();
            } finally {
                w.shutdown();
            }
        });
        
        return w;
    }
    
    @Override
    public Worker createWorker() {
        try {
            return new ConsumerFactoryWorker(factory.call());
        } catch (Exception ex) {
            ExceptionHelper.failUpstream(ex);
            // failUpstream will never complete normally
            return null;
        }
    }
    
    static final class ConsumerFactoryWorker implements Worker, Cancellation, Cancellable {
        final Consumer<Runnable> consumer;
        
        volatile boolean terminated;
        
        public ConsumerFactoryWorker(Consumer<Runnable> consumer) {
            this.consumer = consumer;
        }
        
        @Override
        public Cancellation schedule(Runnable task) {
            Objects.requireNonNull(task, "task");

            if (terminated) {
                return REJECTED;
            }
            
            ExecutorScheduler.ExecutorPlainRunnable r = new ExecutorScheduler.ExecutorPlainRunnable(task);
            
            consumer.accept(r);
            
            return r;
        }
        
        @Override
        public void shutdown() {
            if (!terminated) {
                terminated = true;
                consumer.accept(null);
            }
        }
        
        @Override
        public void dispose() {
            shutdown();
        }
        
        @Override
        public boolean isCancelled() {
            return terminated;
        }
    }
}
