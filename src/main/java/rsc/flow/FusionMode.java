package rsc.flow;

/**
 * Indicates the fusion mode supported.
 */
public enum FusionMode {
    /** Doesn't work with inner Publishers at all. */
    NOT_APPLICABLE,
    /** Does not support fusion. */
    NONE,
    /** Supports only synchronous fusion (with or without boundary marker). */
    SYNC,
    /** Supports only asynchronous fusion (with or without boundary marker). */
    ASYNC,
    /** Supports both synchronous and asynchronous fusion (with or without boundary marker). */
    ANY,
    /** Supports only synchronous fusion but is sensitive to a boundary marker. */
    SYNC_NO_BOUNDARY,
    /** Supports only asynchronous fusion but is sensitive to a boundary marker. */
    ASYNC_NO_BOUNDARY,
    /** Supports both synchronous and asynchronous fusion but is sensitive to a boundary marker. */
    ANY_NO_BOUNDARY
    ;
}
