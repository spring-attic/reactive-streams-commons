package reactivestreams.commons.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;

import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.flow.Receiver;
import reactivestreams.commons.state.Backpressurable;
import reactivestreams.commons.util.ScalarSubscription;

public final class PublisherJust<T> 
extends PublisherBase<T>
implements Fuseable.ScalarSupplier<T>, Receiver, Backpressurable {

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

    @Override
    public long getCapacity() {
        return 1L;
    }
}
