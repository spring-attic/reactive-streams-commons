package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriptions.ScalarDelayedSubscriptionTrait;

/**
 * Emits only the element at the given index position or signals a
 * default value if specified or IndexOutOfBoundsException if the sequence is shorter.
 *
 * @param <T> the value type
 */
public final class PublisherElementAt<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final long index;
    
    final Supplier<? extends T> defaultSupplier;

    public PublisherElementAt(Publisher<? extends T> source, long index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("index >= required but it was " + index);
        }
        this.source = Objects.requireNonNull(source, "source");
        this.index = index;
        this.defaultSupplier = null;
    }

    public PublisherElementAt(Publisher<? extends T> source, long index, Supplier<? extends T> defaultSupplier) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("index >= required but it was " + index);
        }
        this.source = Objects.requireNonNull(source, "source");
        this.index = index;
        this.defaultSupplier = Objects.requireNonNull(defaultSupplier, "defaultSupplier");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherElementAtSubscriber<>(s, index, defaultSupplier));
    }
    
    static final class PublisherElementAtSubscriber<T> 
    implements Subscriber<T>, ScalarDelayedSubscriptionTrait<T> {
        final Subscriber<? super T> actual;
        
        final Supplier<? extends T> defaultSupplier;
        
        long index;
        
        Subscription s;

        boolean done;
        
        T value;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherElementAtSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherElementAtSubscriber.class, "wip");
        
        public PublisherElementAtSubscriber(Subscriber<? super T> actual, long index,
                Supplier<? extends T> defaultSupplier) {
            this.actual = actual;
            this.index = index;
            this.defaultSupplier = defaultSupplier;
        }

        @Override
        public void request(long n) {
            ScalarDelayedSubscriptionTrait.super.request(n);
            if (n > 0L) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void cancel() {
            ScalarDelayedSubscriptionTrait.super.cancel();
            s.cancel();
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
            if (done) {
                return;
            }
            
            long i = index;
            if (i == 0) {
                done = true;
                s.cancel();
                
                actual.onNext(t);
                actual.onComplete();
                return;
            }
            index = i - 1;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            done = true;
            
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

            Supplier<? extends T> ds = defaultSupplier;
            
            if (ds == null) {
                actual.onError(new IndexOutOfBoundsException());
            } else {
                T t;
                
                try {
                    t = ds.get();
                } catch (Throwable e) {
                    actual.onError(e);
                    return;
                }
                
                if (t == null) {
                    actual.onError(new NullPointerException("The defaultSupplier returned a null value"));
                    return;
                }
                
                sdsSet(t);
            }
        }

        @Override
        public int sdsGetState() {
            return wip;
        }

        @Override
        public void sdsSetState(int updated) {
            wip = updated;
        }

        @Override
        public boolean sdsCasState(int expected, int updated) {
            return WIP.compareAndSet(this, expected, updated);
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
    }
}
