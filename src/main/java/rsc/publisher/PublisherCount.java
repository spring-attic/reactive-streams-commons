package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;
import rsc.util.SubscriptionHelper;

/**
 * Counts the number of values in the source sequence.
 *
 * @param <T> the source value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherCount<T> extends PublisherSource<T, Long> implements Fuseable {

    public PublisherCount(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Long> s) {
        source.subscribe(new PublisherCountSubscriber<>(s));
    }

    static final class PublisherCountSubscriber<T> extends DeferredScalarSubscriber<T, Long>
            implements Receiver {

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
