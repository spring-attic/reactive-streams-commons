package rsc.parallel;

import java.util.function.*;
import java.util.stream.Collector;

import org.reactivestreams.*;

import rsc.subscriber.DeferredScalarSubscriber;

import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Reduce the sequence of values in each 'rail' to a single value.
 *
 * @param <T> the input value type
 * @param <A> the accumulated intermedate type
 * @param <R> the result type
 */
public final class ParallelStreamCollect<T, A, R> extends ParallelPublisher<R> {
    
    final ParallelPublisher<? extends T> source;
    
    final Collector<T, A, R> collector;
    
    public ParallelStreamCollect(ParallelPublisher<? extends T> source, 
            Collector<T, A, R> collector) {
        this.source = source;
        this.collector = collector;
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
            
            Supplier<A> initial;
            
            BiConsumer<A, T> coll;
            
            Function<A, R> finisher;
            
            try {
                initial = collector.supplier();
                
                coll = collector.accumulator();
                
                finisher = collector.finisher();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                for (Subscriber<?> s : subscribers) {
                    SubscriptionHelper.error(s, ex);
                }
                return;
            }
            
            A initialValue;
            
            try {
                initialValue = initial.get();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                reportError(subscribers, ex);
                return;
            }
            
            if (initialValue == null) {
                reportError(subscribers, new NullPointerException("The initialSupplier returned a null value"));
                return;
            }
            
            parents[i] = new ParallelStreamCollectSubscriber<>(subscribers[i], initialValue, coll, finisher);
        }
        
        source.subscribe(parents);
    }
    
    void reportError(Subscriber<?>[] subscribers, Throwable ex) {
        for (Subscriber<?> s : subscribers) {
            SubscriptionHelper.error(s, ex);
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

    static final class ParallelStreamCollectSubscriber<T, A, R> extends DeferredScalarSubscriber<T, R> {

        final BiConsumer<A, T> collector;
        
        final Function<A, R> finisher;

        A collection;
        
        Subscription s;

        boolean done;
        
        public ParallelStreamCollectSubscriber(Subscriber<? super R> subscriber, 
                A initialValue, BiConsumer<A, T> collector, Function<A, R> finisher) {
            super(subscriber);
            this.collection = initialValue;
            this.collector = collector;
            this.finisher = finisher;
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
            
            try {
                collector.accept(collection, t);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                cancel();
                onError(ex);
                return;
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            collection = null;
            subscriber.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            A a = collection;
            collection = null;
            
            R r;
            
            try {
                r = finisher.apply(a);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                subscriber.onError(ex);
                return;
            }
            
            if (r == null) {
                subscriber.onError(new NullPointerException("The finisher returned a null value"));
            } else {
                complete(r);
            }
        }
        
        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }
    }
}
