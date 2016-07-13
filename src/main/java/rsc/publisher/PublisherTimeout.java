package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.subscriber.MultiSubscriptionSubscriber;
import rsc.subscriber.SerializedSubscriber;

import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Signals a timeout (or switches to another sequence) in case a per-item
 * generated Publisher source fires an item or completes before the next item
 * arrives from the main source.
 *
 * @param <T> the main source type
 * @param <U> the value type for the timeout for the very first item
 * @param <V> the value type for the timeout for the subsequent items
 */
public final class PublisherTimeout<T, U, V> extends PublisherSource<T, T> {

    final Publisher<U> firstTimeout;

    final Function<? super T, ? extends Publisher<V>> itemTimeout;

    final Publisher<? extends T> other;

    public PublisherTimeout(Publisher<? extends T> source, Publisher<U> firstTimeout,
                            Function<? super T, ? extends Publisher<V>> itemTimeout) {
        super(source);
        this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
        this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
        this.other = null;
    }

    public PublisherTimeout(Publisher<? extends T> source, Publisher<U> firstTimeout,
                            Function<? super T, ? extends Publisher<V>> itemTimeout, Publisher<? extends T> other) {
        super(source);
        this.firstTimeout = Objects.requireNonNull(firstTimeout, "firstTimeout");
        this.itemTimeout = Objects.requireNonNull(itemTimeout, "itemTimeout");
        this.other = Objects.requireNonNull(other, "other");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);

        PublisherTimeoutMainSubscriber<T, V> main = new PublisherTimeoutMainSubscriber<>(serial, itemTimeout, other);

        serial.onSubscribe(main);

        PublisherTimeoutTimeoutSubscriber ts = new PublisherTimeoutTimeoutSubscriber(main, 0L);

        main.setTimeout(ts);

        firstTimeout.subscribe(ts);

        source.subscribe(main);
    }

    static final class PublisherTimeoutMainSubscriber<T, V> extends MultiSubscriptionSubscriber<T, T> {

        final Function<? super T, ? extends Publisher<V>> itemTimeout;

        final Publisher<? extends T> other;

        Subscription s;

        volatile IndexedCancellable timeout;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherTimeoutMainSubscriber, IndexedCancellable> TIMEOUT =
          AtomicReferenceFieldUpdater.newUpdater(PublisherTimeoutMainSubscriber.class, IndexedCancellable.class,
            "timeout");

        volatile long index;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherTimeoutMainSubscriber> INDEX =
          AtomicLongFieldUpdater.newUpdater(PublisherTimeoutMainSubscriber.class, "index");

        public PublisherTimeoutMainSubscriber(Subscriber<? super T> actual,
                                              Function<? super T, ? extends Publisher<V>> itemTimeout,
                                              Publisher<? extends T> other) {
            super(actual);
            this.itemTimeout = itemTimeout;
            this.other = other;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                set(s);
            }
        }

        @Override
        public void onNext(T t) {
            timeout.cancel();

            long idx = index;
            if (idx == Long.MIN_VALUE) {
                s.cancel();
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            if (!INDEX.compareAndSet(this, idx, idx + 1)) {
                s.cancel();
                UnsignalledExceptions.onNextDropped(t);
                return;
            }

            subscriber.onNext(t);

            producedOne();

            Publisher<? extends V> p;

            try {
                p = itemTimeout.apply(t);
            } catch (Throwable e) {
                cancel();
                ExceptionHelper.throwIfFatal(e);
                subscriber.onError(ExceptionHelper.unwrap(e));
                return;
            }

            if (p == null) {
                cancel();

                subscriber.onError(new NullPointerException("The itemTimeout returned a null Publisher"));
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
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            if (!INDEX.compareAndSet(this, idx, Long.MIN_VALUE)) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }

            cancelTimeout();

            subscriber.onError(t);
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

            subscriber.onComplete();
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
            super.cancel();
        }

        boolean setTimeout(IndexedCancellable newTimeout) {

            for (; ; ) {
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
                super.cancel();

                subscriber.onError(e);
            }
        }

        void handleTimeout() {
            if (other == null) {
                super.cancel();

                subscriber.onError(new TimeoutException());
            } else {
                set(SubscriptionHelper.empty());

                other.subscribe(new PublisherTimeoutOtherSubscriber<>(subscriber, this));
            }
        }
        
        @Override
        protected boolean shouldCancelCurrent() {
            return true;
        }
    }

    static final class PublisherTimeoutOtherSubscriber<T> implements Subscriber<T> {

        final Subscriber<? super T> actual;

        final MultiSubscriptionSubscriber<T, T> arbiter;

        public PublisherTimeoutOtherSubscriber(Subscriber<? super T> actual, MultiSubscriptionSubscriber<T, T>
          arbiter) {
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

        final PublisherTimeoutMainSubscriber<?, ?> main;

        final long index;

        volatile Subscription s;

        static final AtomicReferenceFieldUpdater<PublisherTimeoutTimeoutSubscriber, Subscription> S =
          AtomicReferenceFieldUpdater.newUpdater(PublisherTimeoutTimeoutSubscriber.class, Subscription.class, "s");

        public PublisherTimeoutTimeoutSubscriber(PublisherTimeoutMainSubscriber<?, ?> main, long index) {
            this.main = main;
            this.index = index;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!S.compareAndSet(this, null, s)) {
                s.cancel();
                if (this.s != SubscriptionHelper.cancelled()) {
                    SubscriptionHelper.reportSubscriptionSet();
                }
            } else {
                s.request(Long.MAX_VALUE);
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
            if (a != SubscriptionHelper.cancelled()) {
                a = S.getAndSet(this, SubscriptionHelper.cancelled());
                if (a != null && a != SubscriptionHelper.cancelled()) {
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
