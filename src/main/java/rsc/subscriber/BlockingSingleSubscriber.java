package rsc.subscriber;

import java.util.concurrent.*;

import org.reactivestreams.*;

import rsc.flow.Cancellation;
import rsc.util.ExceptionHelper;

public abstract class BlockingSingleSubscriber<T> extends CountDownLatch
implements Subscriber<T>, Cancellation {

    T value;
    Throwable error;
    
    Subscription s;
    
    volatile boolean cancelled;

    public BlockingSingleSubscriber() {
        super(1);
    }

    @Override
    public final void onSubscribe(Subscription s) {
        this.s = s;
        if (!cancelled) {
            s.request(Long.MAX_VALUE);
            if (cancelled) {
                s.cancel();
            }
        }
    }
    
    @Override
    public final void onComplete() {
        countDown();
    }
    
    @Override
    public final void dispose() {
        cancelled = true;
        Subscription s = this.s;
        if (s != null) {
            s.cancel();
        }
    }
    
    /**
     * Block until the first value arrives and return it, otherwise
     * return null for an empty source and rethrow any exception.
     * @return the first value or null if the source is empty
     */
    public final T blockingGet() {
        if (getCount() != 0) {
            try {
                await();
            } catch (InterruptedException ex) {
                dispose();
                throw ExceptionHelper.propagate(ex);
            }
        }
        
        Throwable e = error;
        if (e != null) {
            ExceptionHelper.propagate(e);
        }
        return value;
    }
    
    /**
     * Block until the first value arrives and return it, otherwise
     * return null for an empty source and rethrow any exception.
     * @param timeout the timeout to wait
     * @param unit the time unit
     * @return the first value or null if the source is empty
     */
    public final T blockingGet(long timeout, TimeUnit unit) {
        if (getCount() != 0) {
            try {
                if (!await(timeout, unit)) {
                    dispose();
                }
            } catch (InterruptedException ex) {
                dispose();
                throw ExceptionHelper.propagate(ex);
            }
        }
        
        Throwable e = error;
        if (e != null) {
            ExceptionHelper.propagate(e);
        }
        return value;
    }
}
