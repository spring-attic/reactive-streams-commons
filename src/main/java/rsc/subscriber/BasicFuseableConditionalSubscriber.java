package rsc.subscriber;

import org.reactivestreams.Subscription;

import rsc.flow.Fuseable;
import rsc.flow.Fuseable.*;
import rsc.util.*;

/**
 * Base class for implementing intermediate fuseable operators that
 * are also conditional subscribers.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
public abstract class BasicFuseableConditionalSubscriber<T, R> 
extends BasicConditionalSubscriber<T, R> implements QueueSubscription<R> {
    
    protected QueueSubscription<R> qs;

    /**
     * The established fusion mode, see the Fuseable constants.
     */
    protected int fusionMode;
    
    public BasicFuseableConditionalSubscriber(ConditionalSubscriber<? super R> actual) {
        super(actual);
    }
    
    @Override
    public void onSubscribe(Subscription s) {
        if (SubscriptionHelper.validate(this.s, s)) {
            this.s = s;
            
            this.qs = SubscriptionHelper.as(s);
            
            if (beforeDownstream()) {
                actual.onSubscribe(this);
                
                afterDownstream();
            }
        }
    }
    
    @Override
    public void request(long n) {
        s.request(n);
    }
    
    @Override
    public void cancel() {
        s.cancel();
    }
    
    @Override
    public void clear() {
        qs.clear();
    }
    
    @Override
    public boolean isEmpty() {
        return qs.isEmpty();
    }
    
    @Override
    public int size() {
        return qs.size();
    }
    
    /**
     * If the upstream is fuseable, negotiates the 
     * fusion mode transitively and sets the
     * fusionMode field accordingly.
     * @param mode the fusion mode requested
     * @return the established fusion mode
     */
    protected final int transitiveAnyFusion(int mode) {
        QueueSubscription<R> qs = this.qs;
        if (qs != null) {
            int m = qs.requestFusion(mode);
            if (m != Fuseable.NONE) {
                fusionMode = m;
            }
            return m;
        }
        return Fuseable.NONE;
    }

    /**
     * If the upstream is fuseable, negotiates the 
     * fusion mode transitively but only if the
     * mode doesn't contain the barrier flag and sets the
     * fusionMode field accordingly.
     * @param mode the fusion mode requested
     * @return the established fusion mode
     */
    protected final int transitiveAnyBoundaryFusion(int mode) {
        QueueSubscription<R> qs = this.qs;
        if (qs != null) {
            if ((mode & Fuseable.THREAD_BARRIER) == 0) {
                int m = qs.requestFusion(mode);
                if (m != Fuseable.NONE) {
                    fusionMode = m;
                }
                return m;
            }
        }
        return Fuseable.NONE;
    }

}
