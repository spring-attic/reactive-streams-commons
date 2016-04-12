package rsc.state;

/**
 * A lifecycle backed receiver
 */
public interface Completable {

	/**
	 * Has this upstream started or "onSubscribed" ?
	 * @return has this upstream started or "onSubscribed" ?
	 */
	default boolean isStarted() {
		return false;
	}

	/**
	 * Has this upstream finished or "completed" / "failed" ?
	 * @return has this upstream finished or "completed" / "failed" ?
	 */
	default boolean isTerminated() {
		return false;
	}
}
