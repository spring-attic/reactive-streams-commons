package rsc.parallel;

import java.util.function.*;

import org.reactivestreams.*;

import rsc.subscriber.DeferredScalarSubscriber;
import rsc.util.*;

/**
 * Reduce the sequence of values in each 'rail' to a single value.
 *
 * @param <T> the input value type
 * @param <C> the collection type
 */
public final class ParallelUnorderedCollect<T, C> extends ParallelPublisher<C> {
    
    final ParallelPublisher<? extends T> source;
    
    final Supplier<C> initialCollection;
    
    final BiConsumer<C, T> collector;
    
    public ParallelUnorderedCollect(ParallelPublisher<? extends T> source, 
            Supplier<C> initialCollection, BiConsumer<C, T> collector) {
        this.source = source;
        this.initialCollection = initialCollection;
        this.collector = collector;
    }

    @Override
    public void subscribe(Subscriber<? super C>[] subscribers) {
        if (!validate(subscribers)) {
            return;
        }
        
        int n = subscribers.length;
        @SuppressWarnings("unchecked")
        Subscriber<T>[] parents = new Subscriber[n];
        
        for (int i = 0; i < n; i++) {
            
            C initialValue;
            
            try {
                initialValue = initialCollection.get();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                reportError(subscribers, ex);
                return;
            }
            
            if (initialValue == null) {
                reportError(subscribers, new NullPointerException("The initialSupplier returned a null value"));
                return;
            }
            
            parents[i] = new ParallelCollectSubscriber<>(subscribers[i], initialValue, collector);
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
    public boolean ordered() {
        return source.ordered();
    }

    static final class ParallelCollectSubscriber<T, C> extends DeferredScalarSubscriber<T, C> {

        final BiConsumer<C, T> collector;

        C collection;
        
        Subscription s;

        boolean done;
        
        public ParallelCollectSubscriber(Subscriber<? super C> subscriber, 
                C initialValue, BiConsumer<C, T> collector) {
            super(subscriber);
            this.collection = initialValue;
            this.collector = collector;
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
            C c = collection;
            collection = null;
            complete(c);
        }
        
        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }
    }
}
