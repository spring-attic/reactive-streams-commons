package reactivestreams.commons;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.subscriber.SubscriberDeferScalar;
import reactivestreams.commons.internal.support.SubscriptionHelper;

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
    
    static final class PublisherSingleSubscriber<T> extends SubscriberDeferScalar<T, T> {
        
        final Supplier<? extends T> defaultSupplier;
        
        Subscription s;
        
        int count;
        
        boolean done;
        
        public PublisherSingleSubscriber(Subscriber<? super T> actual, Supplier<? extends T> defaultSupplier) {
            super(actual);
            this.defaultSupplier = defaultSupplier;
        }

        @Override
        public void request(long n) {
            super.request(n);
            if (n > 0L) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }

        public T getValue() {
            return value;
        }

        @Override
        public void setValue(T value) {
            this.value = value;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                subscriber.onSubscribe(this);
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

            subscriber.onError(t);
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
                        subscriber.onError(e);
                        return;
                    }
                    
                    if (t == null) {
                        subscriber.onError(new NullPointerException("The defaultSupplier returned a null value"));
                        return;
                    }
                    
                    set(t);
                } else {
                    subscriber.onError(new NoSuchElementException("Source was empty"));
                }
            } else
            if (c == 1) {
                subscriber.onNext(value);
                subscriber.onComplete();
            }
        }

        
    }
}
