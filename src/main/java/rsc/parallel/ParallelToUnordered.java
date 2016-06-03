package rsc.parallel;

import org.reactivestreams.Subscriber;

/**
 * Removes ordering information from the ordered upstream Parallel publisher.
 *
 * @param <T> the value type
 */
public final class ParallelToUnordered<T> extends ParallelPublisher<T> {

    final ParallelOrderedBase<T> source;

    public ParallelToUnordered(ParallelOrderedBase<T> source) {
        this.source = source;
    }
    
    @Override
    public void subscribe(Subscriber<? super T>[] subscribers) {
        source.subscribe(subscribers);
    }
    
    @Override
    public int parallelism() {
        return source.parallelism();
    }
    
    @Override
    public boolean isOrdered() {
        return false;
    }
}
