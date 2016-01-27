package reactivestreams.commons.state;

/**
 * A component that holds a failure state if any
 */
public interface Failurable {

	Throwable getError();
}
