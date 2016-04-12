package rsc.state;

/**
 * An upstream producer tracker
 */
public interface Prefetchable {

	/**
	 * @return expected number of events to be produced to this component
	 */
	long expectedFromUpstream();


	/**
	 * @return a given limit threshold to replenish outstanding upstream request
	 */
	long limit();
}
