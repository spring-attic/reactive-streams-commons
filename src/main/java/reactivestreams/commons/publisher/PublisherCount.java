package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.subscriber.SubscriberDeferredScalar;
import reactivestreams.commons.trait.Publishable;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * Counts the number of values in the source sequence.
 *
 * @param <T> the source value type
 */
public final class PublisherCount<T> extends PublisherSource<T, Long> {

    public PublisherCount(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Long> s) {
        source.subscribe(new PublisherCountSubscriber<>(s));
    }

    static final class PublisherCountSubscriber<T> extends SubscriberDeferredScalar<T, Long>
            implements Publishable {

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
            complete(counter);
        }

        @Override
        public Object upstream() {
            return s;
        }

    }
}
