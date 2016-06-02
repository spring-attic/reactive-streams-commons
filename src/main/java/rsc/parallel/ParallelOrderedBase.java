package rsc.parallel;

import org.reactivestreams.Subscriber;

/**
 * Base type for ordered parallel sequences and takes care of unordered Subscribers for all subtypes.
 * 
 * @param <T> the value type
 */
public abstract class ParallelOrderedBase<T> extends ParallelPublisher<T> {

    @Override
    public final boolean ordered() {
        return true;
    }
    
    @Override
    public final void subscribe(Subscriber<? super T>[] subscribers) {
        int n = subscribers.length;
        
        @SuppressWarnings("unchecked")
        Subscriber<? super OrderedItem<T>>[] result = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            result[i] = new ParallelUnorderedMap.ParallelMapSubscriber<>(subscribers[i], (OrderedItem<T> v) -> v.get());
        }
        
        subscribeOrdered(result);
    }
    
    public abstract void subscribeOrdered(Subscriber<? super OrderedItem<T>>[] subscribers);
}
