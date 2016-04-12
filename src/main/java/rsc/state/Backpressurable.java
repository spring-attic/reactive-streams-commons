package rsc.state;

/**
 * A storing component
 */
public interface Backpressurable {

	/**
	 * Return defined element capacity
	 * @return long capacity
	 */
	default long getCapacity() {
		return -1L;
	}

	/**
	 * Return current used space in buffer
	 * @return long capacity
	 */
	default long getPending() {
		return -1L;
	}
}
