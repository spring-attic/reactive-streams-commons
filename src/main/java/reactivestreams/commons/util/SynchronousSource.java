package reactivestreams.commons.util;

/**
 * Base class for synchronous sources which have fixed size and can
 * emit its items in a pull fashion, thus avoiding the request-accounting
 * overhead in many cases.
 *
 * @param <T> the content value type
 */
public abstract class SynchronousSource<T> extends QueueFusionBase<T> {
    
}
