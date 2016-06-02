package rsc.parallel;

import java.util.function.Predicate;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Maps each 'rail' of the source ParallelPublisher with a mapper function.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
public final class ParallelOrderedFilter<T> extends ParallelOrderedBase<T> {

    final ParallelOrderedBase<T> source;
    
    final Predicate<? super T> predicate;
    
    public ParallelOrderedFilter(ParallelOrderedBase<T> source, Predicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public void subscribeOrdered(Subscriber<? super OrderedItem<T>>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        Subscriber<? super OrderedItem<T>>[] parents = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            parents[i] = new ParallelFilterSubscriber<>(subscribers[i], predicate);
        }
        
        source.subscribeOrdered(parents);
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    static final class ParallelFilterSubscriber<T> implements Subscriber<OrderedItem<T>>, Subscription {

        final Subscriber<? super OrderedItem<T>> actual;
        
        final Predicate<? super T> predicate;
        
        Subscription s;
        
        boolean done;
        
        public ParallelFilterSubscriber(Subscriber<? super OrderedItem<T>> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }

        @Override
        public void request(long n) {
            s.request(n);
        }

        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(OrderedItem<T> t) {
            if (done) {
                return;
            }
            
            boolean b;
            try {
                b = predicate.test(t.get());
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                cancel();
                onError(ExceptionHelper.unwrap(ex));
                return;
            }
            
            if (b) {
                actual.onNext(t);
            } else {
                s.request(1);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            actual.onComplete();
        }
        
    }
}
