package rsc.parallel;

import java.util.function.*;

import org.reactivestreams.*;

import rsc.util.*;

/**
 * Filters each 'rail' of the source ParallelPublisher with a predicate function.
 *
 * @param <T> the input value type
 */
public final class ParallelUnorderedFilter<T> extends ParallelPublisher<T> {

    final ParallelPublisher<T> source;
    
    final Predicate<? super T> predicate;
    
    public ParallelUnorderedFilter(ParallelPublisher<T> source, Predicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public void subscribe(Subscriber<? super T>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        Subscriber<? super T>[] parents = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            parents[i] = new ParallelFilterSubscriber<>(subscribers[i], predicate);
        }
        
        source.subscribe(parents);
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    @Override
    public boolean isOrdered() {
        return false;
    }
    
    static final class ParallelFilterSubscriber<T> implements Subscriber<T>, Subscription {

        final Subscriber<? super T> actual;
        
        final Predicate<? super T> predicate;
        
        Subscription s;
        
        boolean done;
        
        public ParallelFilterSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
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
        public void onNext(T t) {
            if (done) {
                return;
            }
            boolean b;
            
            try {
                b = predicate.test(t);
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
