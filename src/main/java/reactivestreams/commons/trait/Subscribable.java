package reactivestreams.commons.trait;

/**
 * A component that will emit events to a downstream.
 */
public interface Subscribable {

	/**
	 * Return the direct data receiver.
	 * @return the direct data receiver
	 */
	Object downstream();
}
