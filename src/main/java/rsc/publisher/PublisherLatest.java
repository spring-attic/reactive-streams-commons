package rsc.publisher;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.flow.Producer;
import rsc.flow.Receiver;
import rsc.flow.Trackable;
import rsc.util.BackpressureHelper;
import rsc.subscriber.SubscriptionHelper;

/**
 * Runs the source in unbounded mode and emits only the latest value
 * if the subscriber can't keep up properly.
 *
 * @param <T> the value type
 */
public final class PublisherLatest<T> extends PublisherSource<T, T> {

    public PublisherLatest(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherLatestSubscriber<>(s));
    }

    @Override
    public long getPrefetch() {
        return Long.MAX_VALUE;
    }

    static final class PublisherLatestSubscriber<T>
            implements Subscriber<T>, Subscription, Trackable,
                       Producer,
                       Receiver {

        final Subscriber<? super T> actual;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherLatestSubscriber> REQUESTED =
          AtomicLongFieldUpdater.newUpdater(PublisherLatestSubscriber.class, "requested");

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherLatestSubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherLatestSubscriber.class, "wip");

        Subscription s;

        Throwable error;
        volatile boolean done;

        volatile boolean cancelled;

        volatile T value;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherLatestSubscriber, Object> VALUE =
          AtomicReferenceFieldUpdater.newUpdater(PublisherLatestSubscriber.class, Object.class, "value");

        public PublisherLatestSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.getAndAddCap(REQUESTED, this, n);

                drain();
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {

                cancelled = true;

                s.cancel();

                if (WIP.getAndIncrement(this) == 0) {
                    VALUE.lazySet(this, null);
                }
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);

                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            VALUE.lazySet(this, t);
            drain();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            final Subscriber<? super T> a = actual;

            int missed = 1;

            for (; ; ) {

                if (checkTerminated(done, value == null, a)) {
                    return;
                }

                long r = requested;
                long e = 0L;

                while (r != e) {
                    boolean d = done;

                    @SuppressWarnings("unchecked")
                    T v = (T) VALUE.getAndSet(this, null);

                    boolean empty = v == null;

                    if (checkTerminated(d, empty, a)) {
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);
                    
                    e++;
                }

                if (r == e && checkTerminated(done, value == null, a)) {
                    return;
                }

                if (e != 0L && r != Long.MAX_VALUE) {
                    REQUESTED.addAndGet(this, -e);
                }

                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
            if (cancelled) {
                VALUE.lazySet(this, null);
                return true;
            }

            if (d) {
                Throwable e = error;
                if (e != null) {
                    VALUE.lazySet(this, null);

                    a.onError(e);
                    return true;
                } else if (empty) {
                    a.onComplete();
                    return true;
                }
            }

            return false;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isStarted() {
            return s != null && !cancelled && !done;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public Throwable getError() {
            return error;
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
