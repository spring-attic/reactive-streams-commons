package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.util.BackpressureHelper;
import rsc.util.DeferredSubscription;
import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Takes a value from upstream then uses the duration provided by a 
 * generated Publisher to skip other values until that other Publisher signals.
 *
 * @param <T> the source and output value type
 * @param <U> the value type of the publisher signalling the end of the throttling duration
 */
public final class PublisherThrottleFirst<T, U> extends PublisherSource<T, T> {

    final Function<? super T, ? extends Publisher<U>> throttler;

    public PublisherThrottleFirst(Publisher<? extends T> source,
            Function<? super T, ? extends Publisher<U>> throttler) {
        super(source);
        this.throttler = Objects.requireNonNull(throttler, "throttler");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherThrottleFirstMain<T, U> main = new PublisherThrottleFirstMain<>(s, throttler);
        
        s.onSubscribe(main);
        
        source.subscribe(main);
    }
    
    static final class PublisherThrottleFirstMain<T, U> 
    implements Subscriber<T>, Subscription {

        final Subscriber<? super T> actual;
        
        final Function<? super T, ? extends Publisher<U>> throttler;
        
        volatile boolean gate;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherThrottleFirstMain, Subscription> S =
            AtomicReferenceFieldUpdater.newUpdater(PublisherThrottleFirstMain.class, Subscription.class, "s");

        volatile Subscription other;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherThrottleFirstMain, Subscription> OTHER =
            AtomicReferenceFieldUpdater.newUpdater(PublisherThrottleFirstMain.class, Subscription.class, "other");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherThrottleFirstMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherThrottleFirstMain.class, "requested");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherThrottleFirstMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherThrottleFirstMain.class, "wip");

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherThrottleFirstMain, Throwable> ERROR =
            AtomicReferenceFieldUpdater.newUpdater(PublisherThrottleFirstMain.class, Throwable.class, "error");

        public PublisherThrottleFirstMain(Subscriber<? super T> actual,
                Function<? super T, ? extends Publisher<U>> throttler) {
            this.actual = actual;
            this.throttler = throttler;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);
            }
        }

        @Override
        public void cancel() {
            SubscriptionHelper.terminate(S, this);
            SubscriptionHelper.terminate(OTHER, this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            if (!gate) {
                gate = true;
                
                if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                    actual.onNext(t);
                    if (WIP.decrementAndGet(this) != 0) {
                        handleTermination();
                        return;
                    }
                } else {
                    return;
                }
                
                Publisher<U> p;
                
                try {
                    p = throttler.apply(t);
                } catch (Throwable e) {
                    SubscriptionHelper.terminate(S, this);
                    ExceptionHelper.throwIfFatal(e);
                    error(ExceptionHelper.unwrap(e));
                    return;
                }
                
                if (p == null) {
                    SubscriptionHelper.terminate(S, this);
                    
                    error(new NullPointerException("The throttler returned a null publisher"));
                    return;
                }
                
                PublisherThrottleFirstOther<U> other = new PublisherThrottleFirstOther<>(this);
                
                if (SubscriptionHelper.replace(OTHER, this, other)) {
                    p.subscribe(other);
                }
            }
        }

        void handleTermination() {
            Throwable e = ExceptionHelper.terminate(ERROR, this);
            if (e != null && e != ExceptionHelper.TERMINATED) {
                actual.onError(e);
            } else {
                actual.onComplete();
            }
        }
        
        void error(Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                if (WIP.getAndIncrement(this) == 0) {
                    handleTermination();
                }
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            SubscriptionHelper.terminate(OTHER, this);
            
            error(t);
        }

        @Override
        public void onComplete() {
            SubscriptionHelper.terminate(OTHER, this);
            
            if (WIP.getAndIncrement(this) == 0) {
                handleTermination();
            }
        }
        
        void otherNext() {
            gate = false;
        }
        
        void otherError(Throwable e) {
            SubscriptionHelper.terminate(S, this);
            
            error(e);
        }
    }
    
    static final class PublisherThrottleFirstOther<U>
            extends DeferredSubscription
    implements Subscriber<U> {

        final PublisherThrottleFirstMain<?, U> main;
        
        public PublisherThrottleFirstOther(PublisherThrottleFirstMain<?, U> main) {
            this.main = main;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (set(s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(U t) {
            cancel();
            
            main.otherNext();
        }

        @Override
        public void onError(Throwable t) {
            main.otherError(t);
        }

        @Override
        public void onComplete() {
            main.otherNext();
        }
        
    }
}
