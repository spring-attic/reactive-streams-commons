package reactivestreams.commons;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriptions.ScalarDelayedSubscriptionTrait;

/**
 * Expects and emits a single item from the source or signals
 * NoSuchElementException (or a default generated value) for empty source, 
 * IndexOutOfBoundsException for a multi-item source.
 *
 * @param <T> the value type
 */
public final class PublisherSingle<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final Supplier<? extends T> defaultSupplier;
    
    public PublisherSingle(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
        this.defaultSupplier = null;
    }
    
    public PublisherSingle(Publisher<? extends T> source, Supplier<? extends T> defaultSupplier) {
        this.source = Objects.requireNonNull(source, "source");
        this.defaultSupplier = Objects.requireNonNull(defaultSupplier, "defaultSupplier");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherSingleSubscriber<>(s, defaultSupplier));
    }
    
    static final class PublisherSingleSubscriber<T> implements Subscriber<T>, ScalarDelayedSubscriptionTrait<T> {
        
        final Subscriber<? super T> actual;
        
        final Supplier<? extends T> defaultSupplier;
        
        Subscription s;
        
        int count;
        
        boolean done;
        
        T value;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherSingleSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherSingleSubscriber.class, "wip");


        public PublisherSingleSubscriber(Subscriber<? super T> actual, Supplier<? extends T> defaultSupplier) {
            this.actual = actual;
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
            value = t;
            
            if (++count > 1) {
                cancel();
                
                onError(new IndexOutOfBoundsException("Source emitted more than one item"));
            }
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

            int c = count;
            if (c == 0) {
                Supplier<? extends T> ds = defaultSupplier;
                if (ds != null) {
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
                } else {
                    actual.onError(new NoSuchElementException("Source was empty"));
                }
            } else
            if (c == 1) {
                actual.onNext(value);
                actual.onComplete();
            }
        }

        
    }
}
