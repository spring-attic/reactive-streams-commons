package reactivestreams.commons.util;

import org.reactivestreams.Subscriber;

/**
 * Marker interface indicating the publisher produces a FusionSubscription.
 */
public interface Fuseable {

    /**
     * A subscriber variant that can immediately tell if it consumed
     * the value or not, avoiding the usual request(1) for dropped
     * values.
     *
     * @param <T> the value type
     */
    public interface ConditionalSubscriber<T> extends Subscriber<T> {
        /**
         * Try consuming the value and return true if successful.
         * @param t the value to consume
         * @return true if consumed, false if dropped and a new value can be immediately sent
         */
        boolean tryOnNext(T t);
    }
}
