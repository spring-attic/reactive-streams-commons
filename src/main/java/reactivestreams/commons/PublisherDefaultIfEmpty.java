package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.subscriber.SubscriberScalarDelayed;
import reactivestreams.commons.internal.support.SubscriptionHelper;

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
            extends SubscriberScalarDelayed<T, T> {

        final T value;

        Subscription s;
        
        boolean hasValue;
        
        public PublisherDefaultIfEmptySubscriber(Subscriber<? super T> actual, T value) {
            super(actual);
            this.value = value;
        }

        @Override
        public void request(long n) {
            super.request(n);
            s.request(n);
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
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
            if (!hasValue) {
                hasValue = true;
            }

            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            if (hasValue) {
                subscriber.onComplete();
            } else {
                sdsSet(value);
            }
        }

        @Override
        public T sdsGetValue() {
            return value;
        }

        @Override
        public void sdsSetValue(T value) {
            // value is constant
        }

        
    }
}
