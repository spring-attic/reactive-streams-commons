package reactivestreams.commons.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Dispatches onNext, onError and onComplete signals to zero-to-many Subscribers.
 * 
 * <p>
 * This implementation signals an IllegalStateException if a Subscriber is
 * not ready to receive a value due to not requesting enough.
 *
 * <p>
 * The implementation ignores Subscriptions set via onSubscribe.
 * 
 * <p>
 * A terminated TestProcessor will emit the terminal signal to late subscribers.
 * 
 * @param <T> the input and output value type
 */
public final class TestProcessor<T> implements Processor<T, T> {

    @SuppressWarnings("rawtypes")
    private static final TestProcessorSubscription[] EMPTY = new TestProcessorSubscription[0];
    
    @SuppressWarnings("rawtypes")
    private static final TestProcessorSubscription[] TERMINATED = new TestProcessorSubscription[0];
    
    @SuppressWarnings("unchecked")
    private volatile TestProcessorSubscription<T>[] subscribers = EMPTY;
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<TestProcessor, TestProcessorSubscription[]> SUBSCRIBERS =
            AtomicReferenceFieldUpdater.newUpdater(TestProcessor.class, TestProcessorSubscription[].class, "subscribers");
    
    private Throwable error;
    
    @Override
    public void onSubscribe(Subscription s) {
        Objects.requireNonNull(s, "s");
    }
    
    @Override
    public void onNext(T t) {
        Objects.requireNonNull(t, "t");
        
        for (TestProcessorSubscription<T> s : subscribers) {
            s.onNext(t);
        }
    }
    
    @Override
    public void onError(Throwable t) {
        Objects.requireNonNull(t, "t");

        error = t;
        for (TestProcessorSubscription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
            s.onError(t);
        }
    }
    
    @Override
    public void onComplete() {
        for (TestProcessorSubscription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
            s.onComplete();
        }
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Objects.requireNonNull(s, "s");
        
        TestProcessorSubscription<T> p = new TestProcessorSubscription<>(s, this);
        s.onSubscribe(p);
        
        if (add(p)) {
            if (p.cancelled) {
                remove(p);
            }
        } else {
            Throwable e = error;
            if (e != null) {
                s.onError(e);
            } else {
                s.onComplete();
            }
        }
    }
    
    boolean add(TestProcessorSubscription<T> s) {
        TestProcessorSubscription<T>[] a = subscribers;
        if (a == TERMINATED) {
            return false;
        }
        
        synchronized (this) {
            a = subscribers;
            if (a == TERMINATED) {
                return false;
            }
            int len = a.length;
            
            @SuppressWarnings("unchecked")
            TestProcessorSubscription<T>[] b = new TestProcessorSubscription[len + 1];
            System.arraycopy(a, 0, b, 0, len);
            b[len] = s;
            
            subscribers = b;

            return true;
        }
    }
    
    @SuppressWarnings("unchecked")
    void remove(TestProcessorSubscription<T> s) {
        TestProcessorSubscription<T>[] a = subscribers;
        if (a == TERMINATED || a == EMPTY) {
            return;
        }
        
        synchronized (this) {
            a = subscribers;
            if (a == TERMINATED || a == EMPTY) {
                return;
            }
            int len = a.length;
            
            int j = -1;
            
            for (int i = 0; i < len; i++) {
                if (a[i] == s) {
                    j = i;
                    break;
                }
            }
            if (j < 0) {
                return;
            }
            if (len == 1) {
                subscribers = EMPTY;
                return;
            }

            TestProcessorSubscription<T>[] b = new TestProcessorSubscription[len - 1];
            System.arraycopy(a, 0, b, 0, j);
            System.arraycopy(a, j + 1, b, j, len - j - 1);
            
            subscribers = b;
        }
    }
    
    public boolean hasSubscribers() {
        TestProcessorSubscription<T>[] s = subscribers;
        return s != EMPTY && s != TERMINATED;
    }
    
    public boolean hasCompleted() {
        if (subscribers == TERMINATED) {
            return error == null;
        }
        return false;
    }
    
    public boolean hasError() {
        if (subscribers == TERMINATED) {
            return error != null;
        }
        return false;
    }
    
    public Throwable getError() {
        if (subscribers == TERMINATED) {
            return error;
        }
        return null;
    }
    
    static final class TestProcessorSubscription<T>
    implements Subscription {
        final Subscriber<? super T> actual;
        
        final TestProcessor<T> parent;
        
        volatile boolean cancelled;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<TestProcessorSubscription> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(TestProcessorSubscription.class, "requested");

        public TestProcessorSubscription(Subscriber<? super T> actual, TestProcessor<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(REQUESTED, this, n);
            }
        }
        
        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                parent.remove(this);
            }
        }
        
        void onNext(T value) {
            if (requested != 0L) {
                actual.onNext(value);
                if (requested != Long.MAX_VALUE) {
                    REQUESTED.decrementAndGet(this);
                }
                return;
            }
            parent.remove(this);
            actual.onError(new IllegalStateException("Can't deliver value due to lack of requests"));
        }
        
        void onError(Throwable e) {
            actual.onError(e);
        }
        
        void onComplete() {
            actual.onComplete();
        }
    }
}
