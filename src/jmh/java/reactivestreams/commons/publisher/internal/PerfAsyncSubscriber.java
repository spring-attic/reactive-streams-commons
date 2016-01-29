package reactivestreams.commons.publisher.internal;

import java.util.concurrent.CountDownLatch;

import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.*;

/**
 * Subscriber that relays all events into a black-hole.
 */
public final class PerfAsyncSubscriber implements Subscriber<Object> {

    final Blackhole bh;
    
    final CountDownLatch cdl;

    public PerfAsyncSubscriber(Blackhole bh) {
        this.bh = bh;
        this.cdl = new CountDownLatch(1);
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
        cdl.countDown();
    }

    @Override
    public void onComplete() {
        bh.consume(true);
        cdl.countDown();
    }
    
    public void await(long expected) {
        if (expected < 1000) {
            while (cdl.getCount() != 0);
        } else {
            try {
                cdl.await();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
