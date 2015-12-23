package reactivestream.extensions.internal.subscriptions;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.*;

import reactivestream.extensions.internal.SubscriptionHelper;

public final class ScalarDelayedSubscription<T> implements Subscription {

    final Subscriber<? super T> actual;
    
    T value;

    volatile int state;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<ScalarDelayedSubscription> STATE =
            AtomicIntegerFieldUpdater.newUpdater(ScalarDelayedSubscription.class, "state");

    static final int NO_VALUE_NO_REQUEST = 0;
    static final int HAS_VALUE_NO_REQUEST = 1;
    static final int NO_VALUE_HAS_REQUEST = 2;
    static final int HAS_VALUE_HAS_REQUEST = 3;
    
    public ScalarDelayedSubscription(Subscriber<? super T> actual) {
        this.actual = actual;
    }
    
    public void set(T value) {
        Objects.requireNonNull(value);
        for (;;) {
            int s = state;
            if (s == HAS_VALUE_NO_REQUEST || s == HAS_VALUE_HAS_REQUEST) {
                return;
            }
            if (s == NO_VALUE_HAS_REQUEST) {
                if (STATE.compareAndSet(this, NO_VALUE_HAS_REQUEST, HAS_VALUE_HAS_REQUEST)) {
                    Subscriber<? super T> a = actual;
                    a.onNext(value);
                    a.onComplete();
                }
                return;
            }
            this.value = value;
            if (STATE.compareAndSet(this, NO_VALUE_NO_REQUEST, HAS_VALUE_NO_REQUEST)) {
                return;
            }
        }
    }
    
    @Override
    public void request(long n) {
        if (SubscriptionHelper.validate(n)) {
            for (;;) {
                int s = state;
                if (s == NO_VALUE_HAS_REQUEST || s == HAS_VALUE_HAS_REQUEST) {
                    return;
                }
                if (s == HAS_VALUE_NO_REQUEST) {
                    if (STATE.compareAndSet(this, HAS_VALUE_NO_REQUEST, HAS_VALUE_HAS_REQUEST)) {
                        Subscriber<? super T> a = actual;
                        a.onNext(value);
                        a.onComplete();
                    }
                    return;
                }
                if (STATE.compareAndSet(this, NO_VALUE_NO_REQUEST, NO_VALUE_HAS_REQUEST)) {
                    return;
                }
            }
        }
    }

    @Override
    public void cancel() {
        state = HAS_VALUE_HAS_REQUEST;
    }

}
