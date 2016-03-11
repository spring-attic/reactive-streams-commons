package reactivestreams.commons.scheduler;

/**
 * Represents a cancellable operation.
 */
@FunctionalInterface
public interface Cancellable {
    /**
     * Cancel the operation.
     */
    void cancel();
}
