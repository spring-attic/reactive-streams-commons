package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriptions.ScalarDelayedSubscriptionTrait;

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
    implements Subscriber<T>, ScalarDelayedSubscriptionTrait<T> {

        final Subscriber<? super T> actual;
        
        final T value;

        Subscription s;
        
        boolean hasValue;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherDefaultIfEmptySubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherDefaultIfEmptySubscriber.class, "wip");
        
        public PublisherDefaultIfEmptySubscriber(Subscriber<? super T> actual, T value) {
            this.actual = actual;
            this.value = value;
        }

        @Override
        public void request(long n) {
            ScalarDelayedSubscriptionTrait.super.request(n);
            s.request(n);
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
                sdsSet(value);
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
            // value is constant
        }

        @Override
        public Subscriber<? super T> sdsGetSubscriber() {
            return actual;
        }

        
        
    }
}
