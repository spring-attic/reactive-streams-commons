package reactivestreams.commons.internal;

import java.util.Objects;
import java.util.concurrent.atomic.*;

import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.subscriptions.CancelledSubscription;

/**
 * Arbitrates the requests and cancellation for a single Subscription that
 * may be set later.
 * <p>
 * Note that {@link #request(long)} doesn't validate the amount.
 */
public final class SingleSubscriptionArbiter implements Subscription {
    
    volatile Subscription s;
    static final AtomicReferenceFieldUpdater<SingleSubscriptionArbiter, Subscription> S =
            AtomicReferenceFieldUpdater.newUpdater(SingleSubscriptionArbiter.class, Subscription.class, "s");
    
    volatile long requested;
    static final AtomicLongFieldUpdater<SingleSubscriptionArbiter> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(SingleSubscriptionArbiter.class, "requested");

    /**
     * Constructs a SingleSubscriptionArbiter with zero initial request.
     */
    public SingleSubscriptionArbiter() {
        
    }
    
    /**
     * Constructs a SingleSubscriptionArbiter with the specified initial request amount.
     * @param initialRequest
     * @throws IllegalArgumentException if initialRequest is negative
     */
    public SingleSubscriptionArbiter(long initialRequest) {
        if (initialRequest < 0) {
            throw new IllegalArgumentException("initialRequest >= required but it was " + initialRequest);
        }
        REQUESTED.lazySet(this, initialRequest);
    }
    /**
     * Atomically sets the single subscription and requests the missed
     * amount from it.
     * @param s
     * @return false if this arbiter is cancelled or there was a subscription already set
     */
    public boolean set(Subscription s) {
        Objects.requireNonNull(s, "s");
        Subscription a = this.s;
        if (a == CancelledSubscription.INSTANCE) {
            if (s != null) {
                s.cancel();
            }
            return false;
        }
        if (a != null) {
            if (s != null) {
                s.cancel();
            }
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
            if (s != null) {
                s.cancel();
            }
        }
        
        return false;
    }
    
    @Override
    public void request(long n) {
        Subscription a = s;
        if (a != null) {
            a.request(n);
        } else {
            BackpressureHelper.add(REQUESTED, this, n);
            
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
     * @return true if this arbiter has been cancelled
     */
    public boolean isCancelled() {
        return s == CancelledSubscription.INSTANCE;
    }
    
    /**
     * Returns true if a subscription has been set or the arbiter has been cancelled.
     * <p>
     * Use {@link #isCancelled()} to distinguish between the two states.
     * @return true if a subscription has been set or the arbiter has been cancelled
     */
    public boolean hasSubscription() {
        return s != null;
    }
}
