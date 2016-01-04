package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.subscription.ScalarSubscription;
import reactivestreams.commons.support.ReactiveState;

import java.util.Objects;
import java.util.function.Supplier;

public final class PublisherJust<T> implements Supplier<T>, Publisher<T>,
                                               ReactiveState.Factory,
                                               ReactiveState.Upstream {

    final T value;

    public PublisherJust(T value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    @Override
    public T get() {
        return value;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        s.onSubscribe(new ScalarSubscription<>(s, value));
    }

    @Override
    public Object upstream() {
        return value;
    }
}
