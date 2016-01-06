package reactivestreams.commons.error;

import java.util.function.Consumer;

/**
 * Utility class that let's the developer react to
 * exceptions that can't be signalled due to the state
 * of the streams.
 */
public final class UnsignalledExceptions {

    /**
     * Utility class.
     */
    private UnsignalledExceptions() {
        throw new IllegalStateException("No instances!");
    }

    /**
     * The error consumer lambda, null will revert to the default behavior.
     */
    private static volatile Consumer<Throwable> errorConsumer;

    /**
     * Prevents changing the errorConsumer.
     * This can be used for environments which wants to preset a handler
     * but prevent others from changing it.
     */
    private static volatile boolean locked;

    /**
     * Returns the current error consumer instance or null if none is set.
     * <p>
     * This allows chaining of error consumers if necessary.
     *
     * @return the current error consumer instance or null if none is set
     */
    public static Consumer<Throwable> getErrorConsumer() {
        return errorConsumer;
    }

    /**
     * Sets the current error consumer if not locked down.
     * <p>
     * Setting it to null will reset the handling behavior to default.
     *
     * @param newConsumer the new consumer to set
     */
    public static void setErrorConsumer(Consumer<Throwable> newConsumer) {
        if (!locked) {
            errorConsumer = newConsumer;
        }
    }

    /**
     * Locks down the error consumer and prevents any further changes to
     * the handler.
     */
    public static void lockdown() {
        locked = true;
    }

    /**
     * Take an unsignalled data and handle it.
     *
     * @param <T> the type of the value dropped
     * @param t the dropped data
     */
    public static <T> void onNextDropped(T t) {
    }

    /**
     * Throws a particular {@code Throwable} only if it belongs to a set of "fatal" error varieties. These
     * varieties are as follows:
     * <ul>
     * <li>{@code StackOverflowError}</li>
     * <li>{@code VirtualMachineError}</li>
     * <li>{@code ThreadDeath}</li>
     * <li>{@code LinkageError}</li>
     * </ul>
     *
     * @param t
     */
    public static void throwIfFatal(Throwable t) {
        if (t instanceof StackOverflowError) {
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
     * Take an unsignalled exception and handle it.
     *
     * @param e the exception to handle, if null, a new NullPointerException is instantiated
     */
    public static void onErrorDropped(Throwable e) {
        throwIfFatal(e);
        if (e == null) {
            e = new NullPointerException();
        }

        Consumer<Throwable> h = errorConsumer;

        if (h == null) {
            e.printStackTrace();
        } else {
            try {
                h.accept(e);
            } catch (Throwable ex) {
                ex.printStackTrace();
                e.printStackTrace();
            }
        }
    }
}
