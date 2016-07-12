package rsc.parallel;

import java.util.function.*;

import org.reactivestreams.*;

import rsc.subscriber.DeferredScalarSubscriber;
import rsc.subscriber.EmptySubscription;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Reduce the sequence of values in each 'rail' to a single value.
 *
 * @param <T> the input value type
 * @param <R> the result value type
 */
public final class ParallelReduce<T, R> extends ParallelPublisher<R> {
    
    final ParallelPublisher<? extends T> source;
    
    final Supplier<R> initialSupplier;
    
    final BiFunction<R, T, R> reducer;
    
    public ParallelReduce(ParallelPublisher<? extends T> source, Supplier<R> initialSupplier, BiFunction<R, T, R> reducer) {
        this.source = source;
        this.initialSupplier = initialSupplier;
        this.reducer = reducer;
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
            
            R initialValue;
            
            try {
                initialValue = initialSupplier.get();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                reportError(subscribers, ex);
                return;
            }
            
            if (initialValue == null) {
                reportError(subscribers, new NullPointerException("The initialSupplier returned a null value"));
                return;
            }
            
            parents[i] = new ParallelReduceSubscriber<>(subscribers[i], initialValue, reducer);
        }
        
        source.subscribe(parents);
    }
    
    void reportError(Subscriber<?>[] subscribers, Throwable ex) {
        for (Subscriber<?> s : subscribers) {
            EmptySubscription.error(s, ex);
        }
    }

    @Override
    public int parallelism() {
        return source.parallelism();
    }

    @Override
    public boolean isOrdered() {
        return false;
    }

    static final class ParallelReduceSubscriber<T, R> extends DeferredScalarSubscriber<T, R> {

        final BiFunction<R, T, R> reducer;

        R accumulator;
        
        Subscription s;

        boolean done;
        
        public ParallelReduceSubscriber(Subscriber<? super R> subscriber, R initialValue, BiFunction<R, T, R> reducer) {
            super(subscriber);
            this.accumulator = initialValue;
            this.reducer = reducer;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                subscriber.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            
            R v;
            
            try {
                v = reducer.apply(accumulator, t);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                cancel();
                onError(ex);
                return;
            }
            
            if (v == null) {
                cancel();
                onError(new NullPointerException("The reducer returned a null value"));
                return;
            }
            
            accumulator = v;
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            accumulator = null;
            subscriber.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            
            R a = accumulator;
            accumulator = null;
            complete(a);
        }
        
        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }
    }
}
