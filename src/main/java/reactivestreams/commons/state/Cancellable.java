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
}
