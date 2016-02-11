package reactivestreams.commons.publisher;

import reactivestreams.commons.state.Groupable;

/**
 * Represents a sequence of events with an associated key.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class GroupedPublisher<K, V> extends PublisherBase<V> implements Groupable<K> {

}
