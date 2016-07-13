package rsc.publisher;

import org.reactivestreams.*;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;
import rsc.subscriber.SubscriptionHelper;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherTakeLastOne<T> extends PublisherSource<T, T> implements Fuseable {

    public PublisherTakeLastOne(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherTakeLastOneSubscriber<>(s));
    }


    @Override
    public long getPrefetch() {
        return Long.MAX_VALUE;
    }

    static final class PublisherTakeLastOneSubscriber<T>
            extends DeferredScalarSubscriber<T, T>
            implements Receiver {

        Subscription s;

        public PublisherTakeLastOneSubscriber(Subscriber<? super T> actual) {
            super(actual);
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
            value = t;
        }

        @Override
        public void onComplete() {
            T v = value;
            if (v == null) {
                subscriber.onComplete();
                return;
            }
            complete(v);
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }

        @Override
        public void setValue(T value) {
            // value is always in a field
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
