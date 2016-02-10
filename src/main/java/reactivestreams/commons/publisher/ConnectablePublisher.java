package reactivestreams.commons.publisher;

import java.util.function.Consumer;

/**
 * The abstract base class for connectable publishers that let subscribers pile up
 * before they connect to their data source.
 * 
 * @param <T> the input and output value type
 */
public abstract class ConnectablePublisher<T> extends PublisherBase<T> {

	/**
	 *
     * @return
     */
    public final PublisherBase<T> autoConnect() {
        return autoConnect(1);
    }

	/**
	 *
     * @param minSubscribers
     * @return
     */
    public final PublisherBase<T> autoConnect(int minSubscribers) {
        return autoConnect(minSubscribers, NOOP_DISCONNECT);
    }

	/**
     *
     * @param minSubscribers
     * @param cancelSupport
     * @return
     */
    public final PublisherBase<T> autoConnect(int minSubscribers, Consumer<? super Runnable> cancelSupport) {
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
    public final Runnable connect() {
        final Runnable[] out = { null };
        connect(new Consumer<Runnable>() {
            @Override
            public void accept(Runnable r) {
                out[0] = r;
            }
        });
        return out[0];
    }

    /**
     * Connects this Publisher to its source and sends a Runnable to a callback that
     * can be used for disconnecting.
     * <p>The call should be idempotent in respect of connecting the first
     * and subsequent times. In addition the disconnection should be also tied
     * to a particular connection (so two different connection can't disconnect the other).
     *
     * @param cancelSupport the callback is called with a Runnable instance that can
     * be called to disconnect the source, even synchronously.
     */
    public abstract void connect(Consumer<? super Runnable> cancelSupport);

	/**
	 *
     * @param minSubscribers
     * @return
     */
    public final PublisherBase<T> refCount(int minSubscribers) {
        return new ConnectablePublisherRefCount<>(this, minSubscribers);
    }

	/**
	 *
     * @return
     */
    public final PublisherBase<T> refCount() {
        return refCount(1);
    }

    static final Consumer<Runnable> NOOP_DISCONNECT = new Consumer<Runnable>() {
        @Override
        public void accept(Runnable runnable) {

        }
    };
}
