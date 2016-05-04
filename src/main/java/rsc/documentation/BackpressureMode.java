package rsc.documentation;

/**
 * Indicates the backpressure mode of an operator.
 */
public enum BackpressureMode {
    /** Backpressure is not involved. */
    NOT_APPLICABLE,
    
    /** Backpressure is completely ignored. */
    NONE,
    
    /** 
     * If the downstream can't keep up, an error is signalled
     * and the sequence is terminated.
     */
    ERROR,
    
    /**
     * Requests Long.MAX_VALUE from the upstream and either
     * buffers the values or reduces them to a smaller number.
     */
    UNBOUNDED,
    
    /**
     * Has a fixed prefetch behavior or honors the requests of the downstream.
     */
    BOUNDED
}
