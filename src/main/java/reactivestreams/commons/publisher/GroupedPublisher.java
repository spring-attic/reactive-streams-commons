package reactivestreams.commons.publisher;

/**
 * Represents a sequence of events with an associated key.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class GroupedPublisher<K, V> extends Px<V>{


	/**
	 * Return defined identifier
	 * @return defined identifier
	 */
	@Override
	public abstract K key();
}
