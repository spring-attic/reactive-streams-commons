package reactivestreams.commons.trait;

import java.util.Iterator;

/**
 * A component that is linked to N upstreams producers.
 */
public interface PublishableMany {

	/**
	 * Return the connected sources of data.
	 *
	 * @return the connected sources of data
	 */
	Iterator<?> upstreams();

	/**
	 * @return the number of upstreams
	 */
	long upstreamsCount();
}
