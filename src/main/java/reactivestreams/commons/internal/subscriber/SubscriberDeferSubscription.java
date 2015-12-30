package reactivestreams.commons.internal.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.subscription.CancelledSubscription;
import reactivestreams.commons.internal.support.BackpressureHelper;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Arbitrates the requests and cancellation for a Subscription that may be set onSubscribe once only.
 * <p>
 * Note that {@link #request(long)} doesn't validate the amount.
 */
public class SubscriberDeferSubscription<I, O> implements Subscription, Subscriber<I> {

    protected final Subscriber<? super O> subscriber;

    volatile Subscription s;
    static final AtomicReferenceFieldUpdater<SubscriberDeferSubscription, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(SubscriberDeferSubscription.class, Subscription.class, "s");

    @SuppressWarnings("unused")
    volatile long requested;
    static final AtomicLongFieldUpdater<SubscriberDeferSubscription> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(SubscriberDeferSubscription.class, "requested");

    /**
     * Constructs a SingleSubscriptionArbiter with zero initial request.
     */
    public SubscriberDeferSubscription(Subscriber<? super O> subscriber) {
        this.subscriber = subscriber;
    }

    /**
     * Constructs a SingleSubscriptionArbiter with the specified initial request amount.
     *
     * @param initialRequest
     * @throws IllegalArgumentException if initialRequest is negative
     */
    public SubscriberDeferSubscription(Subscriber<? super O> subscriber, long initialRequest) {
        if (initialRequest < 0) {
            throw new IllegalArgumentException("initialRequest >= required but it was " + initialRequest);
        }
        this.subscriber = subscriber;
        REQUESTED.lazySet(this, initialRequest);
    }

    /**
     * Atomically sets the single subscription and requests the missed amount from it.
     *
     * @param s
     * @return false if this arbiter is cancelled or there was a subscription already set
     */
    public final boolean set(Subscription s) {
        Objects.requireNonNull(s, "s");
        Subscription a = this.s;
        if (a == CancelledSubscription.INSTANCE) {
            s.cancel();
            return false;
        }
        if (a != null) {
            s.cancel();
            return false;
        }

        if (S.compareAndSet(this, null, s)) {

            long r = REQUESTED.getAndSet(this, 0L);

            if (r != 0L) {
                s.request(r);
            }

            return true;
        }

        a = this.s;

        if (a != CancelledSubscription.INSTANCE) {
            s.cancel();
        }

        return false;
    }

    @Override
    public void request(long n) {
        Subscription a = s;
        if (a != null) {
            a.request(n);
        } else {
            BackpressureHelper.addAndGet(REQUESTED, this, n);

            a = s;

            if (a != null) {
                long r = REQUESTED.getAndSet(this, 0L);

                if (r != 0L) {
                    a.request(r);
                }
            }
        }
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

    /**
     * Returns true if this arbiter has been cancelled.
     *
     * @return true if this arbiter has been cancelled
     */
    public final boolean isCancelled() {
        return s == CancelledSubscription.INSTANCE;
    }

    /**
     * Returns true if a subscription has been set or the arbiter has been cancelled.
     * <p>
     * Use {@link #isCancelled()} to distinguish between the two states.
     *
     * @return true if a subscription has been set or the arbiter has been cancelled
     */
    public final boolean hasSubscription() {
        return s != null;
    }

    @Override
    public void onSubscribe(Subscription s) {
        set(s);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onNext(I t) {
        if (subscriber != null) {
            subscriber.onNext((O) t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (subscriber != null) {
            subscriber.onError(t);
        }
    }

    @Override
    public void onComplete() {
        if (subscriber != null) {
            subscriber.onComplete();
        }
    }
}
