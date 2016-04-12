package rsc.publisher;

import java.util.function.Consumer;

import rsc.flow.Cancellation;

/**
 * The abstract base class for connectable publishers that let subscribers pile up
 * before they connect to their data source.
 * 
 * @param <T> the input and output value type
 */
public abstract class ConnectablePublisher<T> extends Px<T> {

	/**
	 * Connects this ConnectablePublisher to the upstream source when the first Subscriber
	 * subscribes.
     * @return a Publisher that connects to the upstream source when the first Subscriber subscribes
     */
    public final Px<T> autoConnect() {
        return autoConnect(1);
    }

	/**
	 * Connects this ConnectablePublisher to the upstream source when the specified amount of
	 * Subscriber subscribes.
	 * <p>
	 * Subscribing and immediately unsubscribing Subscribers also contribute the the subscription count
	 * that triggers the connection.
	 * 
     * @param minSubscribers the minimum number of subscribers
     * @return the Publisher that connects to the upstream source when the given amount of Subscribers subscribe
     */
    public final Px<T> autoConnect(int minSubscribers) {
        return autoConnect(minSubscribers, NOOP_DISCONNECT);
    }

	/**
     * Connects this ConnectablePublisher to the upstream source when the specified amount of
     * Subscriber subscribes and calls the supplied consumer with a runnable that allows disconnecting.
     * @param minSubscribers the minimum number of subscribers
     * @param cancelSupport the consumer that will receive the runnable that allows disconnecting
     * @return the Publisher that connects to the upstream source when the given amount of subscribers subscribed
     */
    public final Px<T> autoConnect(int minSubscribers, Consumer<? super Cancellation> cancelSupport) {
        if (minSubscribers == 0) {
            connect(cancelSupport);
            return this;
        }
        return new ConnectablePublisherAutoConnect<>(this, minSubscribers, cancelSupport);
    }

    /**
     * Connect this Publisher to its source and return a Runnable that
     * can be used for disconnecting.
     * @return the Runnable that allows disconnecting the connection after.
     */
    public final Cancellation connect() {
        final Cancellation[] out = { null };
        connect(r -> out[0] = r);
        return out[0];
    }

    /**
     * Connects this Publisher to its source and sends a Runnable to a callback that
     * can be used for disconnecting.
     * <p>The call should be idempotent in respect of connecting the first
     * and subsequent times. In addition the disconnection should be also tied
     * to a particular connection (so two different connection can't disconnect the other).
     *
     * @param cancelSupport the callback is called with a Cancellation instance that can
     * be called to disconnect the source, even synchronously.
     */
    public abstract void connect(Consumer<? super Cancellation> cancelSupport);

	/**
     * Connects to the upstream source when the given number of Subscriber subscribes and disconnects
     * when all Subscribers cancelled or the upstream source completed.
     * @param minSubscribers the number of subscribers expected to subscribe before connection
     * @return the publisher
     */
    public final Px<T> refCount(int minSubscribers) {
        return new ConnectablePublisherRefCount<>(this, minSubscribers);
    }

	/**
	 * Connects to the upstream source when the first Subscriber subscribes and disconnects
	 * when all Subscribers cancelled or the upstream source completed.
     * @return the publisher
     */
    public final Px<T> refCount() {
        return refCount(1);
    }

    static final Consumer<Cancellation> NOOP_DISCONNECT = runnable -> {

    };
}
