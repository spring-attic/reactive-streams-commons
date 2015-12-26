package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;
import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscribers.SerializedSubscriber;
import reactivestreams.commons.internal.subscriptions.CancelledSubscription;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Signals a timeout (or switches to another sequence) in case a per-item
 * generated Publisher source fires an item or completes before the next item
 * arrives from the main source.
 *
 * @param <T> the main source type
 * @param <U> the value type for the timeout for the very first item
 * @param <V> the value type for the timeout for the subsequent items
 */
public final class PublisherTimeout<T, U, V> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final Supplier<? extends Publisher<U>> firstTimeout;
    
    final Function<? super T, ? extends Publisher<V>> itemTimeout;
    
    final Publisher<? extends T> other;

    public PublisherTimeout(Publisher<? extends T> source, Supplier<? extends Publisher<U>> firstTimeout,
            Function<? super T, ? extends Publisher<V>> itemTimeout) {
        this.source = Objects.requireNonNull(source, "source");
        this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
        this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
        this.other = null;
    }

    public PublisherTimeout(Publisher<? extends T> source, Supplier<? extends Publisher<U>> firstTimeout,
            Function<? super T, ? extends Publisher<V>> itemTimeout, Publisher<? extends T> other) {
        this.source = Objects.requireNonNull(source, "source");
        this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
        this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
        this.other = Objects.requireNonNull(other, "other");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        
        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);
        
        PublisherTimeoutMainSubscriber<T, U, V> main = new PublisherTimeoutMainSubscriber<>(serial, itemTimeout, other);
        
        serial.onSubscribe(main);
        
        Publisher<U> firstPublisher;

        try {
            firstPublisher = firstTimeout.get();
        } catch (Throwable e) {
            serial.onError(e);
            return;
        }
        
        if (firstPublisher == null) {
            serial.onError(new NullPointerException("The firstTimeout returned a null Publisher"));
            return;
        }
        
        PublisherTimeoutTimeoutSubscriber ts = new PublisherTimeoutTimeoutSubscriber(main, 0L);
        
        main.setTimeout(ts);
        
        firstPublisher.subscribe(ts);
        
        source.subscribe(main);
    }
        
    static final class PublisherTimeoutMainSubscriber<T, U, V> implements Subscriber<T>, Subscription {
        final Subscriber<? super T> actual;
        
        final Function<? super T, ? extends Publisher<V>> itemTimeout;
        
        final Publisher<? extends T> other;
        
        final MultiSubscriptionArbiter arbiter;
        
        Subscription s;
        
        volatile IndexedCancellable timeout;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherTimeoutMainSubscriber, IndexedCancellable> TIMEOUT =
                AtomicReferenceFieldUpdater.newUpdater(PublisherTimeoutMainSubscriber.class, IndexedCancellable.class, "timeout");

        volatile long index;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherTimeoutMainSubscriber> INDEX =
                AtomicLongFieldUpdater.newUpdater(PublisherTimeoutMainSubscriber.class, "index");
        
        public PublisherTimeoutMainSubscriber(Subscriber<? super T> actual,
                Function<? super T, ? extends Publisher<V>> itemTimeout,
                Publisher<? extends T> other) {
            this.actual = actual;
            this.itemTimeout = itemTimeout;
            this.other = other;
            this.arbiter = new MultiSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                arbiter.set(s);
            }
        }

        @Override
        public void onNext(T t) {
            timeout.cancel();
            
            long idx = index;
            if (idx == Long.MIN_VALUE) {
                s.cancel();
                return;
            }
            if (!INDEX.compareAndSet(this, idx, idx + 1)) {
                s.cancel();
                return;
            }

            actual.onNext(t);

            arbiter.producedOne();

            Publisher<? extends V> p;
            
            try {
                p = itemTimeout.apply(t);
            } catch (Throwable e) {
                cancel();
                
                actual.onError(e);
                return;
            }
            
            if (p == null) {
                cancel();
                
                actual.onError(new NullPointerException("The itemTimeout returned a null Publisher"));
                return;
            }
            
            PublisherTimeoutTimeoutSubscriber ts = new PublisherTimeoutTimeoutSubscriber(this, idx + 1);
            
            if (!setTimeout(ts)) {
                return;
            }
            
            p.subscribe(ts);
        }

        @Override
        public void onError(Throwable t) {
            long idx = index;
            if (idx == Long.MIN_VALUE) {
                return;
            }
            if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
                return;
            }

            cancelTimeout();
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            long idx = index;
            if (idx == Long.MIN_VALUE) {
                return;
            }
            if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
                return;
            }

            cancelTimeout();
            
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            arbiter.request(n);
        }

        void cancelTimeout() {
            IndexedCancellable s = timeout;
            if (s != CancelledIndexedCancellable.INSTANCE) {
                s = TIMEOUT.getAndSet(this, CancelledIndexedCancellable.INSTANCE);
                if (s != null && s != CancelledIndexedCancellable.INSTANCE) {
                    s.cancel();
                }
            }
        }
        
        @Override
        public void cancel() {
            index = Long.MIN_VALUE;
            cancelTimeout();
            arbiter.cancel();
        }
        
        boolean setTimeout(IndexedCancellable newTimeout) {
            
            for (;;) {
                IndexedCancellable currentTimeout = timeout;
                
                if (currentTimeout == CancelledIndexedCancellable.INSTANCE) {
                    newTimeout.cancel();
                    return false;
                }
                
                if (currentTimeout != null && currentTimeout.index() >= newTimeout.index()) {
                    newTimeout.cancel();
                    return false;
                }
                
                if (TIMEOUT.compareAndSet(this, currentTimeout, newTimeout)) {
                    if (currentTimeout != null) {
                        currentTimeout.cancel();
                    }
                    return true;
                }
            }
        }
        
        void doTimeout(long i) {
            if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
                handleTimeout();
            }
        }
        
        void doError(long i, Throwable e) {
            if (index == i && INDEX.compareAndSet(this, i, Long.MIN_VALUE)) {
                arbiter.cancel();
                
                actual.onError(e);
            } 
        }
        
        void handleTimeout() {
            if (other == null) {
                arbiter.cancel();
                
                actual.onError(new TimeoutException());
            } else {
                arbiter.set(EmptySubscription.INSTANCE);
                
                other.subscribe(new PublisherTimeoutOtherSubscriber<>(actual, arbiter));
            }
        }
    }

    static final class PublisherTimeoutOtherSubscriber<T> implements Subscriber<T> {
        
        final Subscriber<? super T> actual;
        
        final MultiSubscriptionArbiter arbiter;

        public PublisherTimeoutOtherSubscriber(Subscriber<? super T> actual, MultiSubscriptionArbiter arbiter) {
            this.actual = actual;
            this.arbiter = arbiter;
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }
    
    interface IndexedCancellable {
        long index();
        
        void cancel();
    }
    
    enum CancelledIndexedCancellable implements IndexedCancellable {
        INSTANCE;

        @Override
        public long index() {
            return Long.MAX_VALUE;
        }

        @Override
        public void cancel() {
            
        }
        
    }
    
    static final class PublisherTimeoutTimeoutSubscriber implements Subscriber<Object>, IndexedCancellable {
        
        final PublisherTimeoutMainSubscriber<?, ?, ?> main;

        final long index;
        
        volatile Subscription s;

        static final AtomicReferenceFieldUpdater<PublisherTimeoutTimeoutSubscriber, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherTimeoutTimeoutSubscriber.class, Subscription.class, "s");
        
        public PublisherTimeoutTimeoutSubscriber(PublisherTimeoutMainSubscriber<?, ?, ?> main, long index) {
            this.main = main;
            this.index = index;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!S.compareAndSet(this, null, s)) {
                s.cancel();
                if (this.s != CancelledSubscription.INSTANCE) {
                    SubscriptionHelper.reportSubscriptionSet();
                }
            }
        }

        @Override
        public void onNext(Object t) {
            s.cancel();
            
            main.doTimeout(index);
        }

        @Override
        public void onError(Throwable t) {
            main.doError(index, t);
        }

        @Override
        public void onComplete() {
            main.doTimeout(index);
        }

        @Override
        public void cancel() {
            Subscription a = s;
            if (a != CancelledSubscription.INSTANCE) {
                a = S.getAndSet(this, CancelledSubscription.INSTANCE);
                if (a != null && a != CancelledSubscription.INSTANCE) {
                    a.cancel();
                }
            }
        }

        @Override
        public long index() {
            return index;
        }
    }
}
