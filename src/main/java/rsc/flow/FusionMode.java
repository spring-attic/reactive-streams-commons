package rsc.flow;

/**
 * Indicates the fusion mode supported, the mode is
 * considered during subscription time and not during assembly time.
 */
public enum FusionMode {
    /** Doesn't work with inner Publishers at all. */
    NOT_APPLICABLE,
    /** Does not support fusion. */
    NONE,
    /** Supports macro-fusion on any Callable sources (including ScalarCallables). */
    CALLABLE,
    /** Supports macro-fusion only on ScalarCallable sources. */
    SCALAR,
    /** Supports synchronous fusion. */
    SYNC,
    /** Supports asynchronous fusion. */
    ASYNC,
    /** Fusion is sensitive to a boundary marker. */
    BOUNDARY_SENSITIVE,
    /** Supports the specific ConditionalSubscriber. */ 
    CONDITIONAL,
    ;
}
