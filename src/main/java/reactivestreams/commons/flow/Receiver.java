package reactivestreams.commons.flow;

/**
 * A component that is linked to a source producer.
 */
public interface Receiver {

	/**
	 * Return the direct source of data, Supports reference.
	 *
	 * @return the direct source of data, Supports reference.
	 */
	Object upstream();
}
