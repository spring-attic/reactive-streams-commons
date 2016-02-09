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
     * Connect this Publisher to its source and return a Runnable that
     * can be used for disconnecting.
     * @return the Runnable that allows disconnecting the connection after.
     */
    public final Runnable connect() {
        Runnable[] out = { null };
        connect(new Consumer<Runnable>() {
            @Override
            public void accept(Runnable r) {
                out[0] = r;
            }
        });
        return out[0];
    }
    
    public final PublisherBase<T> refCount() {
        return refCount(0);
    }
    
    public final PublisherBase<T> refCount(int minSubscribers) {
        // TODO implement
        throw new UnsupportedOperationException();
    }
    
    public final PublisherBase<T> autoConnect() {
        return autoConnect(1);
    }
    
    public final PublisherBase<T> autoConnect(int minSubscribers) {
        return autoConnect(minSubscribers, r -> { });
    }

    public final PublisherBase<T> autoConnect(int minSubscribers, Consumer<? super Runnable> cancelSupport) {
        if (minSubscribers == 0) {
            connect(cancelSupport);
            return this;
        }
        // TODO implement
        throw new UnsupportedOperationException();
    }
}
