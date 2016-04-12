package rsc.subscriber;

import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.util.UnsignalledExceptions;

/**
 * Subscriber that remembers the last item that can be peeked non-blockingly.
 * 
 * <p>
 * This Subscriber is useful to extract the final value of a synchronous sequence
 * without using latches and blocking.
 *
 * @param <T> the result type
 */
public final class PeekLastSubscriber<T> extends AtomicReference<T> implements Subscriber<T> {
    /** */
    private static final long serialVersionUID = 4970658812242850338L;

    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T t) {
        lazySet(t);
    }

    @Override
    public void onError(Throwable t) {
        UnsignalledExceptions.onErrorDropped(t);
    }

    @Override
    public void onComplete() {
        // deliberately ignored
    }
    
}
