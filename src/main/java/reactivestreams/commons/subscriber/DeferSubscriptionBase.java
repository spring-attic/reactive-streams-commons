package reactivestreams.commons.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import reactivestreams.commons.subscription.CancelledSubscription;
import reactivestreams.commons.support.*;

/**
 * Base class for Subscribers that will receive their Subscriptions at any time yet
 * they need to be cancelled or requested at any time.
 */
public class DeferSubscriptionBase implements Subscription {

    volatile Subscription s;
    static final AtomicReferenceFieldUpdater<DeferSubscriptionBase, Subscription> S =
        AtomicReferenceFieldUpdater.newUpdater(DeferSubscriptionBase.class, Subscription.class, "s");

    volatile long requested;
    static final AtomicLongFieldUpdater<DeferSubscriptionBase> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(DeferSubscriptionBase.class, "requested");

    protected final void setInitialRequest(long n) {
        REQUESTED.lazySet(this, n);
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

        if (a != CancelledSubscription.INSTANCE) {
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

}
