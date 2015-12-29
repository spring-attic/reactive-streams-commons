package reactivestreams.commons;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.support.SubscriptionHelper;
import reactivestreams.commons.internal.subscriber.SubscriberScalarDelayed;

public final class PublisherIsEmpty<T> implements Publisher<Boolean> {

    final Publisher<? extends T> source;

    public PublisherIsEmpty(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
    }
    
    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherIsEmptySubscriber<>(s));
    }
    
    static final class PublisherIsEmptySubscriber<T> extends SubscriberScalarDelayed<T, Boolean> {
        Subscription s;

        public PublisherIsEmptySubscriber(Subscriber<? super Boolean> actual) {
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
            s.cancel();
            
            set(false);
        }

        @Override
        public void onComplete() {
            set(true);
        }
    }
}
