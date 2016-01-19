package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.subscription.DeferredSubscription;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.support.BackpressureHelper;
import reactivestreams.commons.support.ExceptionHelper;
import reactivestreams.commons.support.SubscriptionHelper;
import reactivestreams.commons.support.UnsignalledExceptions;

/**
 * Emits the last value from upstream only if there were no newer values emitted
 * during the time window provided by a publisher for that particular last value.
 *
 * @param <T> the source value type
 * @param <U> the value type of the duration publisher
 */
public final class PublisherThrottleTimeout<T, U> extends PublisherSource<T, T> {

    final Function<? super T, ? extends Publisher<U>> throttler;
    
    final Supplier<Queue<Object>> queueSupplier;

    public PublisherThrottleTimeout(Publisher<? extends T> source,
            Function<? super T, ? extends Publisher<U>> throttler,
                    Supplier<Queue<Object>> queueSupplier) {
        super(source);
        this.throttler = Objects.requireNonNull(throttler, "throttler");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void subscribe(Subscriber<? super T> s) {
        
        Queue<PublisherThrottleTimeoutOther<T, U>> q;
        
        try {
            q = (Queue)queueSupplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        
        if (q == null) {
            EmptySubscription.error(s, new NullPointerException("The queueSupplier returned a null queue"));
            return;
        }
        
        PublisherThrottleTimeoutMain<T, U> main = new PublisherThrottleTimeoutMain<>(s, throttler, q);
        
        s.onSubscribe(main);
        
        source.subscribe(main);
    }
    
    static final class PublisherThrottleTimeoutMain<T, U>
    implements Subscriber<T>, Subscription {
        
        final Subscriber<? super T> actual;
        
        final Function<? super T, ? extends Publisher<U>> throttler;
        
        final Queue<PublisherThrottleTimeoutOther<T, U>> queue;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherThrottleTimeoutMain, Subscription> S =
            AtomicReferenceFieldUpdater.newUpdater(PublisherThrottleTimeoutMain.class, Subscription.class, "s");

        volatile Subscription other;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherThrottleTimeoutMain, Subscription> OTHER =
            AtomicReferenceFieldUpdater.newUpdater(PublisherThrottleTimeoutMain.class, Subscription.class, "other");

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherThrottleTimeoutMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherThrottleTimeoutMain.class, "requested");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherThrottleTimeoutMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherThrottleTimeoutMain.class, "wip");

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherThrottleTimeoutMain, Throwable> ERROR =
            AtomicReferenceFieldUpdater.newUpdater(PublisherThrottleTimeoutMain.class, Throwable.class, "error");

        volatile boolean done;
        
        volatile boolean cancelled;
        
        volatile long index;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherThrottleTimeoutMain> INDEX =
                AtomicLongFieldUpdater.newUpdater(PublisherThrottleTimeoutMain.class, "index");

        public PublisherThrottleTimeoutMain(Subscriber<? super T> actual,
                Function<? super T, ? extends Publisher<U>> throttler,
                        Queue<PublisherThrottleTimeoutOther<T, U>> queue) {
            this.actual = actual;
            this.throttler = throttler;
            this.queue = queue;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                SubscriptionHelper.terminate(S, this);
                SubscriptionHelper.terminate(OTHER, this);
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            long idx = INDEX.incrementAndGet(this);
            
            if (!SubscriptionHelper.set(OTHER, this, EmptySubscription.INSTANCE)) {
                return;
            }
            
            Publisher<U> p;
            
            try {
                p = throttler.apply(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return;
            }

            if (p == null) {
                onError(new NullPointerException("The throttler returned a null publisher"));
                return;
            }
            
            PublisherThrottleTimeoutOther<T, U> os = new PublisherThrottleTimeoutOther<>(this, t, idx);
            
            if (SubscriptionHelper.replace(OTHER, this, os)) {
                p.subscribe(os);
            }
        }

        void error(Throwable t) {
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }
        
        @Override
        public void onError(Throwable t) {
            SubscriptionHelper.terminate(OTHER, this);
            
            error(t);
        }

        @Override
        public void onComplete() {
            Subscription o = other;
            if (o instanceof PublisherThrottleTimeoutOther) {
                PublisherThrottleTimeoutOther<?, ?> os = (PublisherThrottleTimeoutOther<?, ?>) o;
                os.cancel();
                os.onComplete();
            }
            done = true;
            drain();
        }
        
        void otherNext(PublisherThrottleTimeoutOther<T, U> other) {
            queue.offer(other);
            drain();
        }
        
        void otherError(long idx, Throwable e) {
            if (idx == index) {
                SubscriptionHelper.terminate(S, this);
                
                error(e);
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            final Subscriber<? super T> a = actual;
            final Queue<PublisherThrottleTimeoutOther<T, U>> q = queue;
            
            int missed = 1;
            
            for (;;) {
                
                for (;;) {
                    boolean d = done;
                    
                    PublisherThrottleTimeoutOther<T, U> o = q.poll();
                    
                    boolean empty = o == null;
                    
                    if (checkTerminated(d, empty, a, q)) {
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    if (o.index == index) {
                        long r = requested;
                        if (r != 0) {
                            a.onNext(o.value);
                            if (r != Long.MAX_VALUE) {
                                REQUESTED.decrementAndGet(this);
                            }
                        } else {
                            cancel();
                            
                            q.clear();
                            
                            Throwable e = new IllegalStateException("Could not emit value due to lack of requests");
                            ExceptionHelper.addThrowable(ERROR, this, e);
                            e = ExceptionHelper.terminate(ERROR, this);
                            
                            a.onError(e);
                            return;
                        }
                    }
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
            if (cancelled) {
                q.clear();
                return true;
            }
            if (d) {
                Throwable e = ExceptionHelper.terminate(ERROR, this);
                if (e != null && e != ExceptionHelper.TERMINATED) {
                    cancel();
                    
                    q.clear();
                    
                    a.onError(e);
                    return true;
                } else
                if (empty) {
                    
                    a.onComplete();
                    return true;
                }
            }
            return false;
        }
    }
    
    static final class PublisherThrottleTimeoutOther<T, U>
    extends DeferredSubscription
    implements Subscriber<U> {
        final PublisherThrottleTimeoutMain<T, U> main;
        
        final T value;
        
        final long index;
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherThrottleTimeoutOther> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherThrottleTimeoutOther.class, "once");
        

        public PublisherThrottleTimeoutOther(PublisherThrottleTimeoutMain<T, U> main, T value, long index) {
            this.main = main;
            this.value = value;
            this.index = index;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (set(s)) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(U t) {
            if (ONCE.compareAndSet(this, 0, 1)) {
                cancel();
                
                main.otherNext(this);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (ONCE.compareAndSet(this, 0, 1)) {
                main.otherError(index, t);
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }

        @Override
        public void onComplete() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                main.otherNext(this);
            }
        }
    }
}
