package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import reactivestreams.commons.util.ReactiveState;
import reactivestreams.commons.util.ScalarSubscription;

public final class PublisherJust<T> 
extends PublisherBase<T>
implements Supplier<T>,
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
