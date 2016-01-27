package reactivestreams.commons.state;

import reactivestreams.commons.flow.Receiver;

/**
 * A lifecycle backed upstream
 */
public interface Completable extends Receiver {

	/**
	 * @return has this upstream started or "onSubscribed" ?
	 */
	boolean isStarted();

	/**
	 *
	 * @return has this upstream finished or "completed" / "failed" ?
	 */
	boolean isTerminated();
}
