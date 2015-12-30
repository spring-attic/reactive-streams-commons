package reactivestreams.commons;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.support.SubscriptionHelper;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Skips source values while a predicate returns
 * true for the value.
 *
 * @param <T> the value type
 */
public final class PublisherSkipWhile<T> extends PublisherSource<T, T> {

    final Predicate<? super T> predicate;

    public PublisherSkipWhile(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public Publisher<? extends T> source() {
        return source;
    }

    public Predicate<? super T> predicate() {
        return predicate;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherSkipWhileSubscriber<>(s, predicate));
    }

    static final class PublisherSkipWhileSubscriber<T> implements Subscriber<T> {
        final Subscriber<? super T> actual;

        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        public PublisherSkipWhileSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(s);
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
                s.cancel();

                onError(e);

                return;
            }

            if (b) {
                s.request(1);

                return;
            }

            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                return;
            }
            done = true;

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

            actual.onComplete();
        }


    }
}
