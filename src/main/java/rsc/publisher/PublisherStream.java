package rsc.publisher;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.util.EmptySubscription;

/**
 * Emits the contents of a Stream source.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.NOT_APPLICABLE, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NOT_APPLICABLE }, output = { FusionMode.SYNC, FusionMode.CONDITIONAL })
public final class PublisherStream<T>
extends Px<T>
        implements Receiver {

    final Stream<? extends T> stream;

    public PublisherStream(Stream<? extends T> iterable) {
        this.stream = Objects.requireNonNull(iterable, "stream");
    }

    @Override
    public Object upstream() {
        return stream;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Iterator<? extends T> it;

        try {
            it = stream.iterator();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        PublisherIterable.subscribe(s, it);
    }

}
