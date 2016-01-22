package reactivestreams.commons.trait;

/**
 * A lifecycle backed upstream
 */
public interface Completable extends Publishable {

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
