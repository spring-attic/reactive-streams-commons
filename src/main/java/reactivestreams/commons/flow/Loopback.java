package reactivestreams.commons.flow;

/**
 * A component that is forking to a sub-flow given a delegate input and that is consuming from a given delegate
 * output
 */
public interface Loopback {

	Object connectedInput();

	Object connectedOutput();
}
