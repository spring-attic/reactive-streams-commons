package reactivestreams.commons.trait;

/**
 * A component that is forking to a sub-flow given a delegate input and that is consuming from a given delegate
 * output
 */
public interface Connectable {

	Object connectedInput();

	Object connectedOutput();
}
