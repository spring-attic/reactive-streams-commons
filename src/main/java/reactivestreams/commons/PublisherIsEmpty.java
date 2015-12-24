package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.*;

import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriptions.ScalarDelayedSubscription;

public final class PublisherIsEmpty<T> implements Publisher<Boolean> {

    final Publisher<? extends T> source;

    public PublisherIsEmpty(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
    }
    
    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherIsEmptySubscriber<>(s));
    }
    
    static final class PublisherIsEmptySubscriber<T> implements Subscriber<T>, Subscription {
        final Subscriber<? super Boolean> actual;
        
        final ScalarDelayedSubscription<Boolean> delayed;

        Subscription s;

        public PublisherIsEmptySubscriber(Subscriber<? super Boolean> actual) {
            this.actual = actual;
            this.delayed = new ScalarDelayedSubscription<>(actual);
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
            s.cancel();
            
            delayed.set(false);
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            delayed.set(true);
        }
    }
}
