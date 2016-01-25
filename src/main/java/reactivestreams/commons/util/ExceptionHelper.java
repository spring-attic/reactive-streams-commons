package reactivestreams.commons.util;

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

    /**
     * Throws a particular {@code Throwable} only if it belongs to a set of "fatal" error varieties. These
     * varieties are as follows:
     * <ul>
     * <li>{@link UpstreamException}</li>
     * <li>{@code StackOverflowError}</li>
     * <li>{@code VirtualMachineError}</li>
     * <li>{@code ThreadDeath}</li>
     * <li>{@code LinkageError}</li>
     * </ul>
     *
     * @param t
     */
    public static void throwIfFatal(Throwable t) {
        if (t instanceof UpstreamException) {
            throw (UpstreamException) t;
        } else if (t instanceof StackOverflowError) {
            throw (StackOverflowError) t;
        } else if (t instanceof VirtualMachineError) {
            throw (VirtualMachineError) t;
        } else if (t instanceof ThreadDeath) {
            throw (ThreadDeath) t;
        } else if (t instanceof LinkageError) {
            throw (LinkageError) t;
        }
    }

    /**
     * Unwrap a particular {@code Throwable} only if it is a wrapped UpstreamException or DownstreamException
     *
     * @param t the root cause
     */
    public static Throwable unwrap(Throwable t) {
        if (t instanceof ReactiveException) {
            return t.getCause();
        }
        return t;
    }

    /**
     * Throw an unchecked {@link RuntimeException} that will be propagated upstream
     *
     * @param t the root cause
     */
    public static void failUpstream(Throwable t) {
        throwIfFatal(t);
        throw wrapUpstream(t);
    }

    /**
     * Throw an unchecked {@link RuntimeException} that will be propagated upstream
     *
     * @param t the root cause
     */
    public static RuntimeException wrapUpstream(Throwable t) {
        if(t instanceof UpstreamException){
            return (UpstreamException)t;
        }
        return new UpstreamException(t);
    }

    /**
     * Throw an unchecked
     * {@link RuntimeException} that will be propagated downstream through {@link org.reactivestreams.Subscriber#onError(Throwable)}
     *
     * @param t the root cause
     */
    public static void fail(Throwable t) {
        throwIfFatal(t);
        throw wrapDownstream(t);
    }

    /**
     * Throw an unchecked {@link RuntimeException} that will be propagated upstream
     *
     * @param t the root cause
     */
    public static RuntimeException wrapDownstream(Throwable t) {
        if(t instanceof DownstreamException){
            return (DownstreamException)t;
        }
        return new DownstreamException(t);
    }

    /**
     * An exception helper for lambda and other checked-to-unchecked exception wrapping
     */
    static class ReactiveException extends RuntimeException {
        /** */
        private static final long serialVersionUID = -4167553196581090231L;

        public ReactiveException(Throwable cause) {
            super(cause);
        }

        public ReactiveException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return getCause() != null ? getCause().fillInStackTrace() : super.fillInStackTrace();
        }
    }

    /**
     * An exception that is propagated upward and considered as "fatal" as per Reactive Stream limited list of
     * exceptions allowed to bubble
     */
    static final class UpstreamException extends ReactiveException {
        /** */
        private static final long serialVersionUID = 2491425277432776142L;

        public UpstreamException(String message) {
            super(message);
        }

        public UpstreamException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception that is propagated downward through {@link org.reactivestreams.Subscriber#onError(Throwable)}
     */
    static final class DownstreamException extends ReactiveException {

        /** */
        private static final long serialVersionUID = -2025033116568832120L;

        public DownstreamException(String message) {
            super(message);
        }

        public DownstreamException(Throwable cause) {
            super(cause);
        }
    }
}
