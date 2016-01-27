package reactivestreams.commons.flow;

/**
 * A component that will emit events to a downstream.
 */
public interface Producer {

	/**
	 * Return the direct data receiver.
	 * @return the direct data receiver
	 */
	Object downstream();
}
