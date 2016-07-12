package rsc.processor;

import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.publisher.Px;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * A processor that signals/replays the very last received onNext value followed by onCompleted or
 * the terminal signal itself to zero to many Subscribers, including those arriving
 * after the terminal signals.
 * 
 * <p>
 * The implementation ignores onSubscribe if not terminated, cancels the incoming Subscription otherwise.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class AsyncProcessor<T> extends Px<T> implements Processor<T, T>, Fuseable {

    volatile AsyncSubscription<T>[] subscribers;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<AsyncProcessor, AsyncSubscription[]> SUBSCRIBERS =
            AtomicReferenceFieldUpdater.newUpdater(AsyncProcessor.class, AsyncSubscription[].class, "subscribers");

    @SuppressWarnings("rawtypes")
    static final AsyncSubscription[] EMPTY = new AsyncSubscription[0];
    
    @SuppressWarnings("rawtypes")
    static final AsyncSubscription[] TERMINATED = new AsyncSubscription[0];
    
    T value;
    
    Throwable error;
    
    public AsyncProcessor() {
        SUBSCRIBERS.lazySet(this, EMPTY);
    }
    
    @Override
    public void onSubscribe(Subscription s) {
        if (subscribers == TERMINATED) {
            s.cancel();
        } else {
            s.request(Long.MAX_VALUE);
        }
    }

    @Override
    public void onNext(T t) {
        if (t == null) {
            throw new NullPointerException();
        }
        value = t;
    }

    @Override
    public void onError(Throwable t) {
        if (t == null) {
            throw new NullPointerException();
        }
        if (subscribers == TERMINATED) {
            UnsignalledExceptions.onErrorDropped(t);
            return;
        }
        value = null;
        error = t;
        for (AsyncSubscription<?> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
            as.drain(null, t);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onComplete() {
        for (AsyncSubscription<T> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
            as.drain(value, null);
        }
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        AsyncSubscription<T> as = new AsyncSubscription<>(s, this);
        s.onSubscribe(as);
        
        if (add(as)) {
            if (as.isCancelled()) {
                remove(as);
            }
        } else {
            as.drain(value, error);
        }
    }
    
    boolean add(AsyncSubscription<T> as) {
        for (;;) {
            AsyncSubscription<T>[] a = subscribers;
            if (a == TERMINATED) {
                return false;
            }
            int n = a.length;
            @SuppressWarnings("unchecked")
            AsyncSubscription<T>[] b = new AsyncSubscription[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = as;
            if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                return true;
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    void remove(AsyncSubscription<T> as) {
        outer:
        for (;;) {
            AsyncSubscription<T>[] a = subscribers;
            if (a == TERMINATED || a == EMPTY) {
                return;
            }
            int n = a.length;
            for (int i = 0; i < n; i++) {
                if (a[i] == as) {
                    AsyncSubscription<T>[] b;
                    
                    if (n == 1) {
                        b = EMPTY;
                    } else {
                        b = new AsyncSubscription[n - 1];
                        System.arraycopy(a, 0, b, 0, i);
                        System.arraycopy(a, i + 1, b, i, n - i - 1);
                    }
                    
                    if (SUBSCRIBERS.compareAndSet(this, a, b)) {
                        return;
                    }
                    
                    continue outer;
                }
            }
            
            break;
        }
    }
    
    boolean isTerminated() {
        return subscribers == TERMINATED;
    }

    public boolean hasSubscribers() {
        return subscribers.length != 0;
    }
    
    static final class AsyncSubscription<T> implements QueueSubscription<T> {
        final Subscriber<? super T> actual;
        
        final AsyncProcessor<T> parent;
        
        volatile int state;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<AsyncSubscription> STATE =
                AtomicIntegerFieldUpdater.newUpdater(AsyncSubscription.class, "state");

        static final int NO_REQUEST_NO_VALUE = 0;
        static final int HAS_REQUEST_NO_VALUE = 1;
        static final int NO_REQUEST_HAS_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;
        
        /** Indicates the fusion is enabled, doubles as the indicator for value consumed. */
        boolean outputFused;
        
        public AsyncSubscription(Subscriber<? super T> actual, AsyncProcessor<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }
        
        /** Signal the value or error if possible, must be called at most once. */
        void drain(T value, Throwable error) {
            if (error != null) {
                actual.onError(error);
                return;
            } else
            if (value == null) {
                actual.onComplete();
                return;
            }
            for (;;) {
                int s = state;
                if (s == NO_REQUEST_HAS_VALUE || s == HAS_REQUEST_HAS_VALUE) {
                    return;
                } else
                if (s == HAS_REQUEST_NO_VALUE) {
                    // since drain is called at most once, no need for CAS to HAS_REQUEST_HAS_VALUE
                    actual.onNext(value);
                    
                    if (state != HAS_REQUEST_HAS_VALUE) {
                        actual.onComplete();
                    }
                    return;
                } else
                if (STATE.compareAndSet(this, s, NO_REQUEST_HAS_VALUE)) {
                    return;
                }
            }
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                for (;;) {
                    int s = state;
                    if (s == HAS_REQUEST_NO_VALUE || s == HAS_REQUEST_HAS_VALUE) {
                        return;
                    } else
                    if (s == NO_REQUEST_HAS_VALUE) {
                        // make sure only one request() call succeeds
                        if (STATE.compareAndSet(this, s, HAS_REQUEST_HAS_VALUE)) {
                            T v = parent.value;
                            if (v != null) {
                                actual.onNext(v);
                            }
                            actual.onComplete();
                        }
                        return;
                    } else
                    if (STATE.compareAndSet(this, s, HAS_REQUEST_NO_VALUE)) {
                        return;
                    }
                }
            }
        }
        
        @Override
        public void cancel() {
            if (STATE.getAndSet(this, HAS_REQUEST_HAS_VALUE) != HAS_REQUEST_HAS_VALUE) {
                parent.remove(this);
            }
        }
        
        boolean isCancelled() {
            return state == HAS_REQUEST_HAS_VALUE;
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            if ((requestedMode & ASYNC) != 0) {
                outputFused = true;
                return ASYNC;
            }
            return NONE;
        }
        
        @Override
        public T poll() {
            // make sure we read parent.value only after parent terminated
            if (parent.isTerminated()) {
                if (outputFused) {
                    // consume parent.value only once
                    outputFused = false;
                    return parent.value;
                }
            }
            return null;
        }
        
        @Override
        public boolean isEmpty() {
            return !outputFused || parent.value == null;
        }
        
        @Override
        public void clear() {
            outputFused = false;
        }
        
        @Override
        public int size() {
            return isEmpty() ? 0 : 1;
        }
    }
}
