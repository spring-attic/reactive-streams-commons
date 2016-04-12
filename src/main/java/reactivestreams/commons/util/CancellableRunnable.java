package reactivestreams.commons.util;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import reactivestreams.commons.state.Cancellable;

/**
 * Wraps a Runnable and calls run() on it when cancel() is invoked exactly once.
 * <p>This class is stateful.
 */
public final class CancellableRunnable implements Cancellable {
    final Runnable runnable;
    
    volatile int once;
    static final AtomicIntegerFieldUpdater<CancellableRunnable> ONCE =
            AtomicIntegerFieldUpdater.newUpdater(CancellableRunnable.class, "once");
    
    public CancellableRunnable(Runnable future) {
        this.runnable = future;
    }
    
    @Override
    public boolean isCancelled() {
        return once != 0;
    }
    
    @Override
    public void cancel() {
        if (ONCE.compareAndSet(this, 0, 1)) {
            runnable.run();
        }
    }
}
