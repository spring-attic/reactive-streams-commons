package rsc.parallel;

import java.util.Objects;
import java.util.function.*;

import org.reactivestreams.*;


import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Execute a Consumer in each 'rail' for the current element passing through.
 *
 * @param <T> the value type
 */
public final class ParallelUnorderedPeek<T> extends ParallelPublisher<T> {

    final ParallelPublisher<T> source;
    
    final Consumer<? super T> onNext;
    final Consumer<? super T> onAfterNext;
    final Consumer<Throwable> onError;
    final Runnable onComplete;
    final Runnable onAfterTerminated;
    final Consumer<? super Subscription> onSubscribe;
    final LongConsumer onRequest;
    final Runnable onCancel;
    
    public ParallelUnorderedPeek(ParallelPublisher<T> source, 
            Consumer<? super T> onNext,
            Consumer<? super T> onAfterNext,
            Consumer<Throwable> onError,
            Runnable onComplete,
            Runnable onAfterTerminated,
            Consumer<? super Subscription> onSubscribe,
            LongConsumer onRequest,
            Runnable onCancel
    ) {
        this.source = source;
        
        this.onNext = Objects.requireNonNull(onNext, "onNext");
        this.onAfterNext = Objects.requireNonNull(onAfterNext, "onAfterNext");
        this.onError = Objects.requireNonNull(onError, "onError");
        this.onComplete = Objects.requireNonNull(onComplete, "onComplete");
        this.onAfterTerminated = Objects.requireNonNull(onAfterTerminated, "onAfterTerminated");
        this.onSubscribe = Objects.requireNonNull(onSubscribe, "onSubscribe");
        this.onRequest = Objects.requireNonNull(onRequest, "onRequest");
        this.onCancel = Objects.requireNonNull(onCancel, "onCancel");
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
            parents[i] = new ParallelPeekSubscriber<>(subscribers[i], this);
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
    
    static final class ParallelPeekSubscriber<T, R> implements Subscriber<T>, Subscription {

        final Subscriber<? super T> actual;
        
        final ParallelUnorderedPeek<T> parent;
        
        Subscription s;
        
        boolean done;
        
        public ParallelPeekSubscriber(Subscriber<? super T> actual, ParallelUnorderedPeek<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            try {
                parent.onRequest.accept(n);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                UnsignalledExceptions.onErrorDropped(ex);
            }
            s.request(n);
        }

        @Override
        public void cancel() {
            try {
                parent.onCancel.run();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                UnsignalledExceptions.onErrorDropped(ex);
            }
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                try {
                    parent.onSubscribe.accept(s);
                } catch (Throwable ex) {
                    ExceptionHelper.throwIfFatal(ex);
                    s.cancel();
                    actual.onSubscribe(SubscriptionHelper.empty());
                    onError(ex);
                    return;
                }
                
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            
            try {
                parent.onNext.accept(t);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                onError(ex);
                return;
            }
            
            actual.onNext(t);
            
            try {
                parent.onAfterNext.accept(t);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
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
            
            try {
                parent.onError.accept(t);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                ex.addSuppressed(t);
                t = ex;
            }
            actual.onError(t);
            
            try {
                parent.onAfterTerminated.run();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                UnsignalledExceptions.onErrorDropped(ex);
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            try {
                parent.onComplete.run();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                actual.onError(ex);
                return;
            }
            actual.onComplete();
            
            try {
                parent.onAfterTerminated.run();
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                UnsignalledExceptions.onErrorDropped(ex);
            }
        }
        
    }
}
