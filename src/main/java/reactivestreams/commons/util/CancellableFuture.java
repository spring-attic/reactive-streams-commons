package reactivestreams.commons.util;

import java.util.concurrent.Future;

import reactivestreams.commons.state.Cancellable;

/**
 * Wraps a Future and calls cancel(true) on it when cancel() is invoked.
 */
public final class CancellableFuture implements Cancellable {
    final Future<?> future;
    
    public CancellableFuture(Future<?> future) {
        this.future = future;
    }
    
    @Override
    public boolean isCancelled() {
        return future.isDone() || future.isCancelled();
    }
    
    @Override
    public void cancel() {
        future.cancel(true);
    }
}
