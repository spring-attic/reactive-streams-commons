package rsc.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import rsc.flow.Receiver;
import rsc.flow.Trackable;
import rsc.util.BackpressureHelper;

/**
 * Base class for Subscribers that will receive their Subscriptions at any time yet
 * they need to be cancelled or requested at any time.
 */
public class DeferredSubscription
        implements Subscription, Receiver, Trackable {

    volatile Subscription s;
    static final AtomicReferenceFieldUpdater<DeferredSubscription, Subscription> S =
        AtomicReferenceFieldUpdater.newUpdater(DeferredSubscription.class, Subscription.class, "s");

    volatile long requested;
    static final AtomicLongFieldUpdater<DeferredSubscription> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(DeferredSubscription.class, "requested");

    protected final void setInitialRequest(long n) {
        REQUESTED.lazySet(this, n);
    }
    
    /**
     * Sets the Subscription once but does not request anything.
     * @param s the Subscription to set
     * @return true if successful, false if the current subscription is not null
     */
    protected final boolean setWithoutRequesting(Subscription s) {
        Objects.requireNonNull(s, "s");
        for (;;) {
            Subscription a = this.s;
            if (a == SubscriptionHelper.cancelled()) {
                s.cancel();
                return false;
            }
            if (a != null) {
                s.cancel();
                SubscriptionHelper.reportSubscriptionSet();
                return false;
            }
    
            if (S.compareAndSet(this, null, s)) {
                return true;
            }
        }
    }

    /**
     * Requests the deferred amount if not zero.
     */
    protected final void requestDeferred() {
        long r = REQUESTED.getAndSet(this, 0L);

        if (r != 0L) {
            s.request(r);
        }
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
        if (a == SubscriptionHelper.cancelled()) {
            s.cancel();
            return false;
        }
        if (a != null) {
            s.cancel();
            SubscriptionHelper.reportSubscriptionSet();
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

        if (a != SubscriptionHelper.cancelled()) {
            s.cancel();
            return false;
        }
        
        SubscriptionHelper.reportSubscriptionSet();
        return false;
    }

    @Override
    public void request(long n) {
        Subscription a = s;
        if (a != null) {
            a.request(n);
        } else {
            BackpressureHelper.getAndAddCap(REQUESTED, this, n);

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
        if (a != SubscriptionHelper.cancelled()) {
            a = S.getAndSet(this, SubscriptionHelper.cancelled());
            if (a != null && a != SubscriptionHelper.cancelled()) {
                a.cancel();
            }
        }
    }

    /**
     * Returns true if this arbiter has been cancelled.
     *
     * @return true if this arbiter has been cancelled
     */
    @Override
    public final boolean isCancelled() {
        return s == SubscriptionHelper.cancelled();
    }

    @Override
    public final boolean isStarted() {
        return s != null;
    }

    @Override
    public final boolean isTerminated() {
        return isCancelled();
    }

    @Override
    public long requestedFromDownstream() {
        return requested;
    }

    @Override
    public Subscription upstream() {
        return s;
    }

}
