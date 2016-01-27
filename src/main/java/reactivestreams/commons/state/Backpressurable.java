package reactivestreams.commons.state;

/**
 * A storing component
 */
public interface Backpressurable {

	/**
	 * Return defined element capacity
	 * @return long capacity
	 */
	long getCapacity();

	/**
	 * Return current used space in buffer
	 * @return long capacity
	 */
	long getPending();
}
