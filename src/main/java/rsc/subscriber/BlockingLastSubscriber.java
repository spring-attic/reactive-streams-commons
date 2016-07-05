package rsc.subscriber;

/**
 * Blocks until the upstream signals its last value or completes.
 *
 * @param <T> the value type
 */
public final class BlockingLastSubscriber<T> extends BlockingSingleSubscriber<T> {

    @Override
    public void onNext(T t) {
        value = t;
    }

    @Override
    public void onError(Throwable t) {
        value = null;
        error = t;
        countDown();
    }
}
