package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.support.SubscriptionHelper;
import reactivestreams.commons.internal.subscriber.SubscriberDeferScalar;

/**
 * Counts the number of values in the source sequence.
 *
 * @param <T> the source value type
 */
public final class PublisherCount<T> implements Publisher<Long> {
    
    final Publisher<? extends T> source;

    public PublisherCount(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source);
    }
    
    @Override
    public void subscribe(Subscriber<? super Long> s) {
        source.subscribe(new PublisherCountSubscriber<>(s));
    }
    
    static final class PublisherCountSubscriber<T> extends SubscriberDeferScalar<T, Long> {
        
        long counter;
        
        Subscription s;

        public PublisherCountSubscriber(Subscriber<? super Long> actual) {
            super(actual);
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
                
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            counter++;
        }

        @Override
        public void onComplete() {
            set(counter);
        }
    };
}
