package reactivestreams.commons.publisher.internal;

import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Subscriber that relays all events into a black-hole.
 */
public final class PerfSlowPathSubscriber implements Subscriber<Object> {

    final Blackhole bh;

    final long initialRequest;

    public PerfSlowPathSubscriber(Blackhole bh, long initialRequest) {
        this.bh = bh;
        this.initialRequest = initialRequest;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(initialRequest);
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
