package rsc.subscriber;

/**
 * Blocks until the upstream signals its first value or completes.
 *
 * @param <T> the value type
 */
public final class BlockingFirstSubscriber<T> extends BlockingSingleSubscriber<T> {

    @Override
    public void onNext(T t) {
        if (value == null) {
            value = t;
            s.cancel();
            countDown();
        }
    }

    @Override
    public void onError(Throwable t) {
        if (value == null) {
            error = t;
        }
        countDown();
    }
}
