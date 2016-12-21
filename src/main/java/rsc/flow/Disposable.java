package rsc.flow;

/**
 * Indicates that a task or resource can be cancelled/disposed.
 * <p>Call to the dispose method is/should be idempotent.
 */
@FunctionalInterface
public interface Disposable {
    /**
     * Cancel or dispose the underlying task or resource.
     * <p>Call to this method is/should be idempotent.
     */
    void dispose();
}
