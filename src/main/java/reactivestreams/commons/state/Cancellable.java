package reactivestreams.commons.state;

/**
 * A lifecycle backed downstream
 */
public interface Cancellable {

    /**
     *
     * @return has the downstream "cancelled" and interrupted its consuming ?
     */
    boolean isCancelled();

    /**
     * Cancel the stream/tasks.
     */
    void cancel();

    /**
     * A cancelled instance.
     */
    static Cancellable CANCELLED = new Cancellable() {
        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public void cancel() {
            // deliberately no-op
        }

        @Override
        public String toString() {
            return "Cancelled";
        }
    };
}
