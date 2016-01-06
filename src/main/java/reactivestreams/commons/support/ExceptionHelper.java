package reactivestreams.commons.support;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Support for atomically compositing exceptions plus a terminal state indicator
 * that routes all other exceptions to the UnsignalledExceptions handler.
 */
public enum ExceptionHelper {
    ;
    
    /**
     * A singleton instance of a Throwable indicating a terminal state for exceptions,
     * don't leak this!
     */
    public static final Throwable TERMINATED = new Throwable("No further exceptions");
    
    public static <T> boolean addThrowable(AtomicReferenceFieldUpdater<T, Throwable> field, T instance, Throwable exception) {
        for (;;) {
            Throwable current = field.get(instance);
            
            if (current == TERMINATED) {
                return false;
            }
            
            Throwable update;
            if (current == null) {
                update = exception;
            } else {
                update = new Throwable("Multiple exceptions");
                update.addSuppressed(current);
                update.addSuppressed(exception);
            }
            
            if (field.compareAndSet(instance, current, update)) {
                return true;
            }
        }
    }
    
    public static <T> Throwable terminate(AtomicReferenceFieldUpdater<T, Throwable> field, T instance) {
        Throwable current = field.get(instance);
        if (current != TERMINATED) {
            current = field.getAndSet(instance, TERMINATED);
        }
        return current;
    }
}
