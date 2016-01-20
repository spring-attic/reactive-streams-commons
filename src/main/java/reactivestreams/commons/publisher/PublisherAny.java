package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.subscriber.SubscriberDeferredScalar;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;
import reactivestreams.commons.util.UnsignalledExceptions;

/**
 * Emits a single boolean true if any of the values of the source sequence match
 * the predicate.
 * <p>
 * The implementation uses short-circuit logic and completes with true if
 * the predicate matches a value.
 *
 * @param <T> the source value type
 */
public final class PublisherAny<T> extends PublisherSource<T, Boolean> {

    final Predicate<? super T> predicate;

    public PublisherAny(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherAnySubscriber<T>(s, predicate));
    }

    static final class PublisherAnySubscriber<T> extends SubscriberDeferredScalar<T, Boolean>
    implements Upstream {
        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        public PublisherAnySubscriber(Subscriber<? super Boolean> actual, Predicate<? super T> predicate) {
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
                ExceptionHelper.throwIfFatal(e);
                subscriber.onError(ExceptionHelper.unwrap(e));
                return;
            }
            if (b) {
                done = true;
                s.cancel();

                complete(true);
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
            complete(false);
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Object delegateInput() {
            return predicate;
        }
    }
}
