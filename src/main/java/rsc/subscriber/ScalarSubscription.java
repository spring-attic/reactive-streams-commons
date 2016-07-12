package rsc.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;

import rsc.flow.*;
import rsc.flow.Fuseable.*;

public final class ScalarSubscription<T> implements QueueSubscription<T>, Producer, Receiver {

    final Subscriber<? super T> actual;

    final T value;

    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<ScalarSubscription> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(ScalarSubscription.class, "once");

    public ScalarSubscription(Subscriber<? super T> actual, T value) {
        this.value = Objects.requireNonNull(value, "value");
        this.actual = Objects.requireNonNull(actual, "actual");
    }

    @Override
    public final Subscriber<? super T> downstream() {
        return actual;
    }

    @Override
    public void request(long n) {
        if (SubscriptionHelper.validate(n)) {
            if (ONCE.compareAndSet(this, 0, 1)) {
                Subscriber<? super T> a = actual;
                a.onNext(value);
                a.onComplete();
            }
        }
    }

    @Override
    public void cancel() {
        ONCE.lazySet(this, 1);
    }

    @Override
    public Object upstream() {
        return value;
    }
    
    @Override
    public int requestFusion(int requestedMode) {
        if ((requestedMode & Fuseable.SYNC) != 0) {
            return Fuseable.SYNC;
        }
        return 0;
    }
    
    @Override
    public T poll() {
        if (once == 0) {
            ONCE.lazySet(this, 1);
            return value;
        }
        return null;
    }
    
    @Override
    public boolean isEmpty() {
        return once != 0;
    }
    
    @Override
    public int size() {
        return isEmpty() ? 0 : 1;
    }
    
    @Override
    public void clear() {
        ONCE.lazySet(this, 1);
    }
}
