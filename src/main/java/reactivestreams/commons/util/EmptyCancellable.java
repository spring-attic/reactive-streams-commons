package reactivestreams.commons.util;

import reactivestreams.commons.state.Cancellable;

/**
 * A cancellable that does nothing and is stateless.
 * <p>Use it to indicate some state based on object identity.
 */
public final class EmptyCancellable implements Cancellable {
    @Override
    public void cancel() {
        // deliberately no-op
    }
    
    @Override
    public boolean isCancelled() {
        // deliberately stateless
        return false;
    }
}
