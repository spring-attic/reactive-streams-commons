package rsc.parallel;

import java.util.*;
import java.util.function.*;

import org.reactivestreams.*;

import rsc.publisher.PublisherConcatMap;
import rsc.publisher.PublisherConcatMap.ErrorMode;

/**
 * Concatenates the generated Publishers on each rail.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
public final class ParallelUnorderedConcatMap<T, R> extends ParallelPublisher<R> {

    final ParallelPublisher<T> source;
    
    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final Supplier<? extends Queue<T>> queueSupplier;
    
    final int prefetch;
    
    final ErrorMode errorMode;

    public ParallelUnorderedConcatMap(
            ParallelPublisher<T> source, 
            Function<? super T, ? extends Publisher<? extends R>> mapper, 
                    Supplier<? extends Queue<T>> queueSupplier,
                    int prefetch, ErrorMode errorMode) {
        this.source = source;
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
        this.prefetch = prefetch;
        this.errorMode = Objects.requireNonNull(errorMode, "errorMode");
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
            parents[i] = PublisherConcatMap.subscribe(subscribers[i], mapper, queueSupplier, prefetch, errorMode);
        }
        
        source.subscribe(parents);
    }
}
