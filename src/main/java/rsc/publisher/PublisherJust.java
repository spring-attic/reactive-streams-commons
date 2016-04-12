package rsc.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;

import rsc.flow.Fuseable;
import rsc.flow.Receiver;
import rsc.state.Backpressurable;
import rsc.util.ScalarSubscription;

public final class PublisherJust<T> 
extends Px<T>
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
