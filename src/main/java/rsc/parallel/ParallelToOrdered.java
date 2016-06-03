package rsc.parallel;

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.*;

/**
 * Applies local or grobal ordering to the regular values from a ParallelPublisher.
 *
 * @param <T> the value type
 */
public final class ParallelToOrdered<T> extends ParallelOrderedBase<T> {

    final ParallelPublisher<T> source;
    
    final boolean globalOrder;

    public ParallelToOrdered(ParallelPublisher<T> source, boolean globalOrder) {
        this.source = source;
        this.globalOrder = globalOrder;
    }
    
    @Override
    public int parallelism() {
        return source.parallelism();
    }
    
    @Override
    public void subscribeOrdered(Subscriber<? super OrderedItem<T>>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        Subscriber<T>[] parents = new Subscriber[n];
        if (globalOrder) {
            AtomicLong index = new AtomicLong();
            for (int i = 0; i < n; i++) {
                parents[i] = new GlobalOrderingSubscriber<>(subscribers[i], index);
            }
        } else {
            for (int i = 0; i < n; i++) {
                parents[i] = new LocalOrderingSubscriber<>(subscribers[i]);
            }
        }
        source.subscribe(parents);
    }
    
    static final class LocalOrderingSubscriber<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super OrderedItem<T>> actual;
        
        Subscription s;
        
        long index;

        public LocalOrderingSubscriber(Subscriber<? super OrderedItem<T>> actual) {
            this.actual = actual;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            actual.onSubscribe(s);
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(PrimaryOrderedItem.of(t, index++));
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
    
    static final class GlobalOrderingSubscriber<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super OrderedItem<T>> actual;
        
        Subscription s;
        
        final AtomicLong index;

        public GlobalOrderingSubscriber(Subscriber<? super OrderedItem<T>> actual, AtomicLong index) {
            this.actual = actual;
            this.index = index;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            actual.onSubscribe(s);
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(PrimaryOrderedItem.of(t, index.getAndIncrement()));
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
