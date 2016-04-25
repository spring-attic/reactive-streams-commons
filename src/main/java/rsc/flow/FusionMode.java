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
    /** Supports macro-fusion with both Callable and ScalarCallable sources. */
    SCALAR,
    /** Supports synchronous fusion. */
    SYNC,
    /** Supports asynchronous fusion. */
    ASYNC,
    /** 
     * Fusion is sensitive to a boundary marker.
     * <p> 
     * If applied to an input, it means the requestFusion call to upstream will have the
     * {@link Fuseable#THREAD_BARRIER} flag.
     * <p>
     * If applied to an output, it means the incoming requestFusion call is checked against the
     * {@link Fuseable#THREAD_BARRIER} flag and will result in (likely) rejection of the fusion.
     */
    BOUNDARY,
    /** Supports the specific ConditionalSubscriber. */ 
    CONDITIONAL,
    ;
}
