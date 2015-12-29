package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.ScalarDelayedArbiter;

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
    
    static final class PublisherCountSubscriber<T> implements Subscriber<T>, Subscription {
        
        final Subscriber<? super Long> actual;
        
        final ScalarDelayedArbiter<Long> delayed;
        
        long counter;
        
        Subscription s;

        public PublisherCountSubscriber(Subscriber<? super Long> actual) {
            this.actual = actual;
            this.delayed = new ScalarDelayedArbiter<>(actual);
        }

        @Override
        public void request(long n) {
            delayed.request(n);
        }

        @Override
        public void cancel() {
            delayed.cancel();
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            counter++;
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            delayed.set(counter);
        }
    };
}
