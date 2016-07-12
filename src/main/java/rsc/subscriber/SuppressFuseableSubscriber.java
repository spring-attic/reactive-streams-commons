package rsc.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.flow.Fuseable;

/**
 * Wraps a Subscriber and suppresses any QueueSubscription capability the upstream might have.
 * 
 * @param <T> the value type
 */
public final class SuppressFuseableSubscriber<T> implements Subscriber<T>, Fuseable.QueueSubscription<T> {
    
    final Subscriber<? super T> actual;

    Subscription s;
    
    public SuppressFuseableSubscriber(Subscriber<? super T> actual) {
        this.actual = actual;
        
    }
    
    @Override
    public void onSubscribe(Subscription s) {
        if (SubscriptionHelper.validate(this.s, s)) {
            this.s = s;
            
            actual.onSubscribe(this);
        }
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
    
    @Override
    public void request(long n) {
        s.request(n);
    }
    
    @Override
    public void cancel() {
        s.cancel();
    }
    
    @Override
    public int requestFusion(int requestedMode) {
        return Fuseable.NONE;
    }
    
    @Override
    public T poll() {
        return null;
    }
    
    @Override
    public boolean isEmpty() {
        return false;
    }
    
    @Override
    public void clear() {
        
    }
    
    @Override
    public int size() {
        return 0;
    }
}
