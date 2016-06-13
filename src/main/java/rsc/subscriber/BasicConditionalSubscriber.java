package rsc.subscriber;

import org.reactivestreams.Subscription;

import rsc.util.*;

import rsc.flow.Fuseable.*;

/**
 * Base class for implementing intermediate fuseable operators.
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
public abstract class BasicConditionalSubscriber<T, R> implements ConditionalSubscriber<T>, Subscription {

    protected final ConditionalSubscriber<? super R> actual;
    
    protected Subscription s;
    
    /**
     * Indicates further onXXX signals should be ignored.
     */
    protected boolean done;
    
    public BasicConditionalSubscriber(ConditionalSubscriber<? super R> actual) {
        this.actual = actual;
    }
    
    /**
     * Calls onComplete on the downstream
     * subscriber if not already done.
     */
    protected final void complete() {
        if (done) {
            return;
        }
        done = true;
        actual.onComplete();
    }
    
    /**
     * Calls onError on the subscriber if not
     * already done.
     * @param ex
     */
    protected final void error(Throwable ex) {
        if (done) {
            UnsignalledExceptions.onErrorDropped(ex);
            return;
        }
        done = true;
        actual.onError(ex);
    }
    
    /**
     * Throws if the exception is fatal, otherwise cancels
     * the subscription and calls error().
     * @param ex
     */
    protected final void fail(Throwable ex) {
        ExceptionHelper.throwIfFatal(ex);
        s.cancel();
        error(ex);
    }
    
    @Override
    public void onSubscribe(Subscription s) {
        if (SubscriptionHelper.validate(this.s, s)) {
            this.s = s;
            
            if (beforeDownstream()) {
                actual.onSubscribe(this);
                
                afterDownstream();
            }
        }
    }
    
    /**
     * Called after the Subscriptions have been set and if
     * returns true, this instance is submitted to the actual
     * downstream subscriber.
     * @return true if should continue
     */
    protected boolean beforeDownstream() {
        return true;
    }
    
    /**
     * Called after this instance has been submitted to the
     * actual downstream subscriber.
     */
    protected void afterDownstream() {
        
    }
    
    @Override
    public void request(long n) {
        s.request(n);
    }
    
    @Override
    public void cancel() {
        s.cancel();
    }
}
