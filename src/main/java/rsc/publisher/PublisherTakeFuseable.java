package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.flow.BackpressureMode;
import rsc.flow.BackpressureSupport;
import rsc.flow.Fuseable;
import rsc.flow.FusionMode;
import rsc.flow.FusionSupport;
import rsc.publisher.PublisherTake.PublisherTakeFuseableSubscriber;

/**
 * Takes only the first N values from the source Publisher.
 * <p>
 * If N is zero, the subscriber gets completed if the source completes, signals an error or
 * signals its first value (which is not not relayed though).
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL }, output = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL })
public final class PublisherTakeFuseable<T> extends PublisherSource<T, T> implements Fuseable {

    final long n;

    public PublisherTakeFuseable(Publisher<? extends T> source, long n) {
        super(source);
        if (n < 0) {
            throw new IllegalArgumentException("n >= 0 required but it was " + n);
        }
        this.n = n;
    }

    public Publisher<? extends T> source() {
        return source;
    }

    public long n() {
        return n;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherTakeFuseableSubscriber<>(s, n));
    }

    @Override
    public long getCapacity() {
        return n;
    }
}
