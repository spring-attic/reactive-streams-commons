package reactivestreams.commons.flow;

/**
 * A component that is forking to a sub-flow given a delegate input and that is consuming from a given delegate
 * output
 */
public interface Loopback {

	/**
	 * @return component delegated to for incoming data or {@code null} if unavailable
	 */
	default Object connectedInput() {
		return null;
	}

	/**
	 * @return component delegated to for outgoing data or {@code null} if unavailable
	 */
	default Object connectedOutput() {
		return null;
	}
}
