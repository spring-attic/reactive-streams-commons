package rsc.subscriber;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.*;

import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * An empty subscriber that ignores onNext events, sends onError events to UnsignalledExceptions
 * and allows asynchronous cancellation.
 *
 * @param <T> the value type
 */
public final class EmptyAsyncSubscriber<T> implements Subscriber<T>, Runnable {

    volatile Subscription s;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<EmptyAsyncSubscriber, Subscription> S =
            AtomicReferenceFieldUpdater.newUpdater(EmptyAsyncSubscriber.class, Subscription.class, "s");
    
    @Override
    public void run() {
        SubscriptionHelper.terminate(S, this);
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (SubscriptionHelper.setOnce(S, this, s)) {
            s.request(Long.MAX_VALUE);
        }
    }

    @Override
    public void onNext(T t) {
        // deliberately ignored
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
