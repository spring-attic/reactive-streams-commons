package reactivestreams.commons.internal.subscriptions;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;

public final class ScalarDelayedSubscription<T> implements ScalarDelayedSubscriptionTrait<T> {

    final Subscriber<? super T> actual;
    
    T value;

    volatile int state;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<ScalarDelayedSubscription> STATE =
            AtomicIntegerFieldUpdater.newUpdater(ScalarDelayedSubscription.class, "state");

    public ScalarDelayedSubscription(Subscriber<? super T> actual) {
        this.actual = actual;
    }

    @Override
    public int sdsGetState() {
        return state;
    }

    @Override
    public void sdsSetState(int updated) {
        state = updated;
    }

    @Override
    public boolean sdsCasState(int expected, int updated) {
        return STATE.compareAndSet(this, expected, updated);
    }

    @Override
    public T sdsGetValue() {
        return value;
    }

    @Override
    public void sdsSetValue(T value) {
        this.value = value;
    }

    @Override
    public Subscriber<? super T> sdsGetSubscriber() {
        return actual;
    }
    
    public void set(T value) {
        sdsSet(value);
    }
}
