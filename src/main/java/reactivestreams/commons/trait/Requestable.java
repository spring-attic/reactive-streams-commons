package reactivestreams.commons.trait;

/**
 * A request aware component
 */
public interface Requestable {

	/**
	 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
	 * This is the maximum in-flight data allowed to transit to this elements.
	 * @return long capacity
	 */
	long requestedFromDownstream();
}
