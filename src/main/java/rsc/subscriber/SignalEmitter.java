package rsc.subscriber;

import org.reactivestreams.Subscriber;
import rsc.state.Backpressurable;
import rsc.state.Cancellable;
import rsc.state.Introspectable;
import rsc.state.Requestable;
import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Interface to receive generated signals from the callback function.
 * <p>
 * At least one of the methods should be called per invocation of the generator function
 *
 * @param <T> the output value type
 */
public interface SignalEmitter<T> extends Backpressurable, Introspectable, Cancellable,
                                          Requestable {

    /**
     * @see {@link Subscriber#onComplete()}
     */
    void complete();

    /**
     * @see {@link Subscriber#onNext(Object)}
     */
    Emission emit(T t);

    /**
     * @see {@link Subscriber#onError(Throwable)}
     */
    void fail(Throwable e);

    /**
     * Indicate there won't be any further signals delivered by
     * the generator and the operator will stop calling it.
     * <p>
     * Call to this method will also trigger the state consumer.
     */
    void stop();

    /**
     * Try emitting or throw an unchecked exception.
     *
     * @see #emit(Object)
     * @throws RuntimeException
     */
    default void tryEmit(T t) {
        Emission emission = emit(t);
        if(emission.isOk()) {
            return;
        }
        if(emission.isBackpressured()){
            SubscriptionHelper.reportMoreProduced();
            return;
        }
        if(emission.isCancelled()){
            UnsignalledExceptions.onNextDropped(t);
            return;
        }
        if(getError() != null){
            throw ExceptionHelper.bubble(getError());
        }
        throw new IllegalStateException("Emission has failed");
    }

    /**
     * An acknowledgement signal returned by {@link #emit}.
     * {@link Emission#isOk()} is the only successful signal, the other define the emission failure cause.
     *
     */
    enum Emission {
        FAILED, BACKPRESSURED, OK, CANCELLED;

        public boolean isBackpressured(){
            return this == BACKPRESSURED;
        }

        public boolean isCancelled(){
            return this == CANCELLED;
        }

        public boolean isFailed(){
            return this == FAILED;
        }

        public boolean isOk(){
            return this == OK;
        }
    }
}
