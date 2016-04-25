package rsc.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;

import rsc.flow.*;
import rsc.state.Backpressurable;
import rsc.util.ScalarSubscription;

/**
 * Emits exactly one, non-null value synchronously.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.NONE, output = BackpressureMode.BOUNDED)
@FusionSupport(output = { FusionMode.SCALAR })
public final class PublisherJust<T> 
extends Px<T>
implements Fuseable.ScalarCallable<T>, Receiver, Backpressurable {

    final T value;

    public PublisherJust(T value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    @Override
    public T call() {
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
