package reactivestreams.commons.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.util.UnsignalledExceptions;

public enum DropAllSubscriber implements Subscriber<Object> {
    INSTANCE;

    @SuppressWarnings("unchecked")
    public static <T> Subscriber<T> instance() {
        return (Subscriber<T>) INSTANCE;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Object t) {
        // deliberately no op
    }

    @Override
    public void onError(Throwable t) {
        UnsignalledExceptions.onErrorDropped(t);
    }

    @Override
    public void onComplete() {
        // deliberately no op
    }

}
