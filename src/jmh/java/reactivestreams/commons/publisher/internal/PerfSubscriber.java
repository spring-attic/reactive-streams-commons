package reactivestreams.commons.publisher.internal;

import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Subscriber that relays all events into a black-hole.
 */
public final class PerfSubscriber implements Subscriber<Object> {

    final Blackhole bh;

    public PerfSubscriber(Blackhole bh) {
        this.bh = bh;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Object t) {
        bh.consume(t);
    }

    @Override
    public void onError(Throwable t) {
        bh.consume(t);
    }

    @Override
    public void onComplete() {
        bh.consume(true);
    }
}
