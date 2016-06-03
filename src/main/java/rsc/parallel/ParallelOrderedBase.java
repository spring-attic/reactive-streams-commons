package rsc.parallel;

import org.reactivestreams.*;

/**
 * Base type for ordered parallel sequences and takes care of unordered Subscribers for all subtypes.
 * 
 * @param <T> the value type
 */
public abstract class ParallelOrderedBase<T> extends ParallelPublisher<T> {

    @Override
    public final boolean isOrdered() {
        return true;
    }
    
    @Override
    public final void subscribe(Subscriber<? super T>[] subscribers) {
        int n = subscribers.length;
        
        @SuppressWarnings("unchecked")
        Subscriber<? super OrderedItem<T>>[] result = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            result[i] = new RemoveOrderedSubscriber<>(subscribers[i]);
        }
        
        subscribeOrdered(result);
    }
    
    /**
     * Subscribe with an array of order-aware subscribers.
     * 
     * @param subscribers the array of order-aware subscribers
     */
    public abstract void subscribeOrdered(Subscriber<? super OrderedItem<T>>[] subscribers);
    
    /**
     * Unwraps the item from the upstream's OrderedItem.
     *
     * @param <T> the value type
     */
    static final class RemoveOrderedSubscriber<T> implements Subscriber<OrderedItem<T>>, Subscription {

        final Subscriber<? super T> actual;

        Subscription s;
        
        public RemoveOrderedSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            actual.onSubscribe(this);
        }
        
        @Override
        public void onNext(OrderedItem<T> t) {
            actual.onNext(t.get());
        }
        
        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            actual.onComplete();
        }
        
        @Override
        public void request(long n) {
            s.request(n);
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }
    }
}
