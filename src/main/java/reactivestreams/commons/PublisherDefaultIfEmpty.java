package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.*;

import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Emits a scalar value if the source sequence turns out to be empty.
 *
 * @param <T> the value type
 */
public final class PublisherDefaultIfEmpty<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final T value;

    public PublisherDefaultIfEmpty(Publisher<? extends T> source, T value) {
        this.source = Objects.requireNonNull(source, "source");
        this.value = Objects.requireNonNull(value, "value");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherDefaultIfEmptySubscriber<>(s, value));
    }
    
    static final class PublisherDefaultIfEmptySubscriber<T> 
    extends AtomicInteger
    implements Subscriber<T>, Subscription {
        
        /** */
        private static final long serialVersionUID = -1144745940026178027L;

        final Subscriber<? super T> actual;
        
        final T value;

        Subscription s;
        
        boolean hasValue;
        
        static final int NO_REQUEST_NO_VALUE = 0;
        static final int HAS_REQUEST_NO_VALUE = 1;
        static final int NO_REQUEST_HAS_VALUE = 2;
        static final int HAS_REQUEST_HAS_VALUE = 3;
        
        public PublisherDefaultIfEmptySubscriber(Subscriber<? super T> actual, T value) {
            super();
            this.actual = actual;
            this.value = value;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                s.request(n);
                
                for (;;) {
                    int state = get();
                    if (state == HAS_REQUEST_NO_VALUE || state == HAS_REQUEST_HAS_VALUE) {
                        return;
                    }
                    if (state == NO_REQUEST_HAS_VALUE) {
                        if (compareAndSet(NO_REQUEST_HAS_VALUE, HAS_REQUEST_HAS_VALUE)) {
                            actual.onNext(value);
                            actual.onComplete();
                        }
                        return;
                    }
                    if (compareAndSet(NO_REQUEST_NO_VALUE, HAS_REQUEST_NO_VALUE)) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            getAndSet(HAS_REQUEST_HAS_VALUE);
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
            if (!hasValue) {
                hasValue = true;
            }
            
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (hasValue) {
                actual.onComplete();
            } else {
                for (;;) {
                    int state = get();
                    
                    if (state == NO_REQUEST_HAS_VALUE || state == HAS_REQUEST_HAS_VALUE) {
                        return;
                    }
                    if (state == HAS_REQUEST_NO_VALUE) {
                        if (compareAndSet(HAS_REQUEST_NO_VALUE, HAS_REQUEST_HAS_VALUE)) {
                            actual.onNext(value);
                            actual.onComplete();
                        }
                        return;
                    }
                    if (compareAndSet(NO_REQUEST_NO_VALUE, NO_REQUEST_HAS_VALUE)) {
                        return;
                    }
                }
            }
        }

        
    }
}
