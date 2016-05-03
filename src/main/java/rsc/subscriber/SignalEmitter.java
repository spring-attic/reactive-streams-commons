package rsc.subscriber;

/**
 * Interface to receive generated signals from the callback function.
 * <p>
 * Methods of this interface should be called at most once per invocation
 * of the generator function. In addition, at least one of the methods
 * should be called per invocation of the generator function
 *
 * @param <T> the output value type
 */
public interface SignalEmitter<T> {

    void onNext(T t);

    void onError(Throwable e);

    void onComplete();

    /**
     * Indicate there won't be any further signals delivered by
     * the generator and the operator will stop calling it.
     * <p>
     * Call to this method will also trigger the state consumer.
     */
    void stop();
}
