package reactivestreams.commons.util;

import org.reactivestreams.Subscription;

/**
 * Base class for asynchronous sources which can act as a queue and subscription
 * at the same time, saving on allocating another queue most of the time.
 * 
 * <p>
 * Implementation note: even though it looks exactly like SynchronousSource, this
 * class has to be separate because the protocol is different (i.e., SynchronousSource should
 * be never requested).
 * 
 * @param <T> the content value type
 */
public abstract class AsynchronousSource<T> extends QueueFusionBase<T> implements Subscription {

    /**
     * Consumers of an AsynchronousSource have to signal it to switch to a fused-mode
     * so it no longer run its own drain loop but directly signals onNext(null) to 
     * indicate there is an item available in this queue-view.
     * <p>
     * The method has to be called while the parent is in onSubscribe and before any
     * other interaction with the Subscription.
     */
    public abstract void enableOperatorFusion();
}
