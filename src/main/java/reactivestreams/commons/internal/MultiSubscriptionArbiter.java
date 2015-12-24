package reactivestreams.commons.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;

/**
 * A subscription implementation that arbitrates request amounts between subsequent Subscriptions,
 * including the duration until the first Subscription is set.
 * <p>
 * The class is thread safe but switching Subscriptions should happen only when the source associated
 * with the current Subscription has finished emitting values. Otherwise, two sources may emit for one request.
 * <p>
 * You should call {@link #produced(long)} or {@link #producedOne()} after each element has been delivered
 * to properly account the outstanding request amount in case a Subscription switch happens.
 */
public final class MultiSubscriptionArbiter implements Subscription {

    /** The current subscription which may null if no Subscriptions have been set. */
    Subscription actual;
    
    /** The current outstanding request amount. */
    long requested;
    
    volatile Subscription missedSubscription;
    static final AtomicReferenceFieldUpdater<MultiSubscriptionArbiter, Subscription> MISSED_SUBSCRIPTION =
            AtomicReferenceFieldUpdater.newUpdater(MultiSubscriptionArbiter.class, Subscription.class, "missedSubscription");
    
    
    volatile long missedRequested;
    static final AtomicLongFieldUpdater<MultiSubscriptionArbiter> MISSED_REQUESTED =
            AtomicLongFieldUpdater.newUpdater(MultiSubscriptionArbiter.class, "missedRequested");
    
    volatile long missedProduced;
    static final AtomicLongFieldUpdater<MultiSubscriptionArbiter> MISSED_PRODUCED =
            AtomicLongFieldUpdater.newUpdater(MultiSubscriptionArbiter.class, "missedProduced");
    
    volatile int wip;
    static final AtomicIntegerFieldUpdater<MultiSubscriptionArbiter> WIP =
            AtomicIntegerFieldUpdater.newUpdater(MultiSubscriptionArbiter.class, "wip");

    volatile boolean cancelled;
    
    public void set(Subscription s) {
        if (cancelled) {
            return;
        }
        
        Objects.requireNonNull(s);
        
        Subscription a = MISSED_SUBSCRIPTION.getAndSet(this, s);
        if (a != null) {
            a.cancel();
        }
        drain();
    }
    
    @Override
    public void request(long n) {
        if (SubscriptionHelper.validate(n)) {
            
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;
                
                if (r != Long.MAX_VALUE) {
                    requested = BackpressureHelper.addCap(r, n);
                }
                Subscription a = actual;
                if (a != null) {
                    a.request(n);
                }
                
                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }
                
                drainLoop();
                
                return;
            }
            
            BackpressureHelper.add(MISSED_REQUESTED, this, n);
            
            drain();
        }
    }

    public void producedOne() {
        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
            long r = requested;
            
            if (r != Long.MAX_VALUE) {
                r--;
                if (r < 0L) {
                    SubscriptionHelper.reportMoreProduced();
                    r = 0;
                }
                requested = r;
            }
            
            if (WIP.decrementAndGet(this) == 0) {
                return;
            }
            
            drainLoop();
            
            return;
        }
        
        BackpressureHelper.add(MISSED_PRODUCED, this, 1L);
        
        drain();
    }

    
    public void produced(long n) {
        if (SubscriptionHelper.validate(n)) {
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;
                
                if (r != Long.MAX_VALUE) {
                    long u = r - n;
                    if (u < 0L) {
                        SubscriptionHelper.reportMoreProduced();
                        u = 0;
                    }
                    requested = u;
                }
                
                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }
                
                drainLoop();
                
                return;
            }
            
            BackpressureHelper.add(MISSED_PRODUCED, this, n);
            
            drain();
        }
    }
    
    @Override
    public void cancel() {
        if (!cancelled) {
            cancelled = true;

            drain();
        }
    }
    
    public boolean isCancelled() {
        return cancelled;
    }

    void drain() {
        if (WIP.getAndIncrement(this) != 0) {
            return;
        }
        drainLoop();
    }
    
    void drainLoop() {
        int missed = 1;
        
        for (;;) {
            
            Subscription ms = missedSubscription;
            
            if (ms != null) {
                ms = MISSED_SUBSCRIPTION.getAndSet(this, null);
            }
            
            long mr = missedRequested;
            if (mr != 0L) {
                mr = MISSED_REQUESTED.getAndSet(this, 0L);
            }

            long mp = missedProduced;
            if (mp != 0L) {
                mp = MISSED_PRODUCED.getAndSet(this, 0L);
            }

            Subscription a = actual;

            if (cancelled) {
                if (a != null) {
                    a.cancel();
                    actual = null;
                }
                if (ms != null) {
                    ms.cancel();
                }
            } else {
                long r = requested;
                if (r != Long.MAX_VALUE) {
                    long u = BackpressureHelper.addCap(r, mr);
                    
                    if (u != Long.MAX_VALUE) {
                        long v = u - mp;
                        if (v < 0L) {
                            SubscriptionHelper.reportMoreProduced();
                            v = 0;
                        }
                        r = v;
                    } else {
                        r = u;
                    }
                    requested =  r;
                }
                
                if (ms != null) {
                    if (a != null) {
                        a.cancel();
                    }
                    actual = ms;
                    if (r != 0L) {
                        ms.request(r);
                    }
                } else
                if (mr != 0L && a != null) {
                    a.request(mr);
                }
            }
            
            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                return;
            }
        }
    }
}
