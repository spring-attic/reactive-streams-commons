package reactivestreams.commons.trait;

/**
 * A component that holds a failure state if any
 */
public interface Failurable {

	Throwable getError();
}
