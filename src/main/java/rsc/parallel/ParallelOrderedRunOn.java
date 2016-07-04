package rsc.parallel;

import java.util.Queue;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;

import rsc.parallel.ParallelUnorderedRunOn.RunOnSubscriber;
import rsc.scheduler.Scheduler;
import rsc.scheduler.Scheduler.Worker;

/**
 * Ensures each 'rail' from upstream runs on a Worker from a Scheduler.
 *
 * @param <T> the value type
 */
public final class ParallelOrderedRunOn<T> extends ParallelOrderedBase<T> {
    final ParallelOrderedBase<T> source;
    
    final Scheduler scheduler;

    final int prefetch;

    final Supplier<Queue<OrderedItem<T>>> queueSupplier;
    
    public ParallelOrderedRunOn(ParallelOrderedBase<T> parent, 
            Scheduler scheduler, int prefetch, Supplier<Queue<OrderedItem<T>>> queueSupplier) {
        this.source = parent;
        this.scheduler = scheduler;
        this.prefetch = prefetch;
        this.queueSupplier = queueSupplier;
    }
    
    @Override
    public void subscribeOrdered(Subscriber<? super OrderedItem<T>>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        int n = subscribers.length;
        
        @SuppressWarnings("unchecked")
        Subscriber<OrderedItem<T>>[] parents = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            Subscriber<? super OrderedItem<T>> a = subscribers[i];
            
            Worker w = scheduler.createWorker();
            Queue<OrderedItem<T>> q = queueSupplier.get();
            
            RunOnSubscriber<OrderedItem<T>> parent = new RunOnSubscriber<>(a, prefetch, q, w);
            parents[i] = parent;
        }
        
        source.subscribeOrdered(parents);
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }
}
