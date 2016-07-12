package rsc.subscriber;

import org.reactivestreams.Subscriber;
import rsc.util.ExceptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Interface to receive generated signals from the callback function.
 * <p>
 * At least one of the methods should be called per invocation of the generator function
 *
 * @param <T> the output value type
 */
public interface SignalEmitter<T> extends SubscriberState {

    /**
     * Signal the completion of the sequence.
     * @see Subscriber#onComplete()
     */
    void complete();

    /**
     * Signal the next value in the sequence.
     * @param t the value to signal, not-null
     * @see Subscriber#onNext(Object)
     * @return the result of the emission, see {@link Emission} enum.
     */
    Emission emit(T t);

    /**
     * Signal and terminate the sequence with an error.
     * @param e the Throwable instance, not-null
     * @see Subscriber#onError(Throwable)
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
     * @param t the value to emit.
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
     * {@link SignalEmitter.Emission#isOk()} is the only successful signal, the other define the emission failure cause.
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
