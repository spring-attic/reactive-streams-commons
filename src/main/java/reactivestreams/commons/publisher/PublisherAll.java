package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.*;

import reactivestreams.commons.error.UnsignalledExceptions;
import reactivestreams.commons.subscriber.SubscriberDeferredScalar;
import reactivestreams.commons.support.SubscriptionHelper;

/**
 * Emits a single boolean true if all values of the source sequence match
 * the predicate.
 * <p>
 * The implementation uses short-circuit logic and completes with false if
 * the predicate doesn't match a value.
 *
 * @param <T> the source value type
 */
public final class PublisherAll<T> extends PublisherSource<T, Boolean> {

    final Predicate<? super T> predicate;

    public PublisherAll(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherAllSubscriber<T>(s, predicate));
    }

    static final class PublisherAllSubscriber<T> extends SubscriberDeferredScalar<T, Boolean>
    implements Upstream {
        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        public PublisherAllSubscriber(Subscriber<? super Boolean> actual, Predicate<? super T> predicate) {
            super(actual);
            this.predicate = predicate;
        }

        @Override
        public void cancel() {
            s.cancel();
            super.cancel();
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

            if (done) {
                return;
            }

            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                done = true;
                s.cancel();

                subscriber.onError(e);
                return;
            }
            if (!b) {
                done = true;
                s.cancel();

                set(false);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;

            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            set(true);
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Object delegateInput() {
            return predicate;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }
    }
}
