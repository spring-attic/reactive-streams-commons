package rsc.parallel;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Wraps multiple Publishers into a ParallelPublisher which runs them
 * in parallel.
 * 
 * @param <T> the value type
 */
public final class ParallelUnorderedFrom<T> extends ParallelPublisher<T> {
    final Publisher<T>[] sources;
    
    public ParallelUnorderedFrom(Publisher<T>[] sources) {
        this.sources = sources;
    }
    
    @Override
    public boolean ordered() {
        return false;
    }
    
    @Override
    public int parallelism() {
        return sources.length;
    }
    
    @Override
    public void subscribe(Subscriber<? super T>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        int n = subscribers.length;
        
        for (int i = 0; i < n; i++) {
            sources[i].subscribe(subscribers[i]);
        }
    }
}
