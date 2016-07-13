
package rsc.publisher;

/**
 * A component that has introspection options
 */
public interface PublisherConfig {

    /**
     * Defined identifier or null if not available
     * @return defined identifier or null if not available
     */
    default Object getId() {
        return null;
    }

    /**
     * The prefetch configuration of the component
     * @return the prefetch configuration of the component
     */
    default long getPrefetch() {
        return -1L;
    }
}
