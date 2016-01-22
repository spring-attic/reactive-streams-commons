package reactivestreams.commons.trait;

/**
 * A component that is linked to a source producer.
 */
public interface Publishable {

	/**
	 * Return the direct source of data, Supports reference.
	 *
	 * @return the direct source of data, Supports reference.
	 */
	Object upstream();
}
