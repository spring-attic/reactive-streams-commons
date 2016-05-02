package rsc.flow;

import java.util.Iterator;

/**
 * A component that will emit events to N downstreams.
 */
public interface MultiProducer {

	/**
	 * the connected data receivers
	 * @return the connected data receivers
	 */
	Iterator<?> downstreams();

	/**
	 * the number of downstream receivers
	 * @return the number of downstream receivers
	 */
	default long downstreamCount() {
		return -1L;
	}

	/**
	 * Has any Subscriber attached to this multi-producer ?
	 * @return Has any Subscriber attached to this multi-producer ?
	 */
	default boolean hasDownstreams() {
		return downstreamCount() != 0;
	}

}
