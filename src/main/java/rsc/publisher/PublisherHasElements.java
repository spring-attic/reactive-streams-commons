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
import rsc.subscriber.SubscriptionHelper;

@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherHasElements<T> extends PublisherSource<T, Boolean> {

    public PublisherHasElements(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherHasElementsSubscriber<>(s));
    }

    @Override
    public long getPrefetch() {
        return Long.MAX_VALUE;
    }

    static final class PublisherHasElementsSubscriber<T> extends DeferredScalarSubscriber<T, Boolean>
            implements Receiver {
        Subscription s;

        public PublisherHasElementsSubscriber(Subscriber<? super Boolean> actual) {
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

            complete(true);
        }

        @Override
        public void onComplete() {
            complete(false);
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
