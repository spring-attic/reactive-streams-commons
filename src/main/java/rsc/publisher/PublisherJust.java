package rsc.publisher;

import java.util.Objects;

import org.reactivestreams.Subscriber;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.flow.Trackable;
import rsc.subscriber.ScalarSubscription;

/**
 * Emits exactly one, non-null value synchronously.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.NONE, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NOT_APPLICABLE}, output = { FusionMode.SCALAR, FusionMode.SYNC })
public final class PublisherJust<T> 
extends Px<T>
implements Fuseable.ScalarCallable<T>, Receiver, Fuseable, Trackable {

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
