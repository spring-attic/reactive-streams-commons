package reactivestreams.commons.util;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import reactivestreams.commons.scheduler.Scheduler;
import reactivestreams.commons.util.ExecutorScheduler.ExecutorPlainRunnable;

/**
 * Wraps the old Callable&lt;Consumer&lt;Runnable>> based API into a scheduler.
 */
public final class ConsumerFactoryScheduler implements Scheduler {
    
    final Callable<Consumer<Runnable>> factory;
    
    public ConsumerFactoryScheduler(Callable<Consumer<Runnable>> factory) {
        this.factory = factory;
    }
    
    @Override
    public Runnable schedule(Runnable task) {
        Objects.requireNonNull(task, "task");
        Worker w = createWorker();
        
        w.schedule(() -> {
            try {
                task.run();
            } finally {
                w.shutdown();
            }
        });
        
        return w::shutdown;
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
    
    static final class ConsumerFactoryWorker implements Worker {
        final Consumer<Runnable> consumer;
        
        volatile boolean terminated;
        
        public ConsumerFactoryWorker(Consumer<Runnable> consumer) {
            this.consumer = consumer;
        }
        
        @Override
        public Runnable schedule(Runnable task) {
            Objects.requireNonNull(task, "task");

            if (terminated) {
                return REJECTED;
            }
            
            ExecutorPlainRunnable r = new ExecutorPlainRunnable(task);
            
            consumer.accept(r);
            
            return r::cancel;
        }
        
        @Override
        public void shutdown() {
            if (!terminated) {
                terminated = true;
                consumer.accept(null);
            }
        }
    }
}
