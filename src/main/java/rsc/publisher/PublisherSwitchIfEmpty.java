package rsc.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rsc.flow.Loopback;
import rsc.subscriber.MultiSubscriptionSubscriber;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 */
public final class PublisherSwitchIfEmpty<T> extends PublisherSource<T, T> {

    final Publisher<? extends T> other;

    public PublisherSwitchIfEmpty(Publisher<? extends T> source, Publisher<? extends T> other) {
        super(source);
        this.other = Objects.requireNonNull(other, "other");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherSwitchIfEmptySubscriber<T> parent = new PublisherSwitchIfEmptySubscriber<>(s, other);

        s.onSubscribe(parent);

        source.subscribe(parent);
    }

    static final class PublisherSwitchIfEmptySubscriber<T> extends MultiSubscriptionSubscriber<T, T>
            implements Loopback {

        final Publisher<? extends T> other;

        boolean once;

        public PublisherSwitchIfEmptySubscriber(Subscriber<? super T> actual, Publisher<? extends T> other) {
            super(actual);
            this.other = other;
        }

        @Override
        public void onNext(T t) {
            if (!once) {
                once = true;
            }

            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            if (!once) {
                once = true;

                other.subscribe(this);
            } else {
                subscriber.onComplete();
            }
        }

        @Override
        public Object connectedInput() {
            return null;
        }

        @Override
        public Object connectedOutput() {
            return other;
        }
    }
}
