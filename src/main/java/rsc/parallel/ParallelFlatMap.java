package rsc.parallel;

import java.util.Queue;
import java.util.function.*;

import org.reactivestreams.*;

import rsc.publisher.PublisherFlatMap;

/**
 * Flattens the generated Publishers on each rail.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
public final class ParallelFlatMap<T, R> extends ParallelPublisher<R> {

    final ParallelPublisher<T> source;
    
    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final boolean delayError;
    
    final int maxConcurrency;
    
    final Supplier<? extends Queue<R>> mainQueueSupplier;

    final int prefetch;
    
    final Supplier<? extends Queue<R>> innerQueueSupplier;

    public ParallelFlatMap(
            ParallelPublisher<T> source, 
            Function<? super T, ? extends Publisher<? extends R>> mapper,
            boolean delayError, 
            int maxConcurrency, Supplier<? extends Queue<R>> mainQueueSupplier, 
            int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
        this.source = source;
        this.mapper = mapper;
        this.delayError = delayError;
        this.maxConcurrency = maxConcurrency;
        this.mainQueueSupplier = mainQueueSupplier;
        this.prefetch = prefetch;
        this.innerQueueSupplier = innerQueueSupplier;
    }
    
    @Override
    public boolean isOrdered() {
        return false;
    }
    
    @Override
    public int parallelism() {
        return source.parallelism();
    }
    
    @Override
    public void subscribe(Subscriber<? super R>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        int n = subscribers.length;
        
        @SuppressWarnings("unchecked")
        Subscriber<T>[] parents = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            parents[i] = PublisherFlatMap.subscribe(subscribers[i], mapper, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier);
        }
        
        source.subscribe(parents);
    }
}
