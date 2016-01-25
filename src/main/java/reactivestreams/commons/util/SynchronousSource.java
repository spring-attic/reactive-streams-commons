package reactivestreams.commons.util;

import java.util.*;
import java.util.function.Consumer;

/**
 * Base class for synchronous sources which have fixed size and can
 * emit its items in a pull fashion, thus avoiding the request-accounting
 * overhead in many cases.
 *
 * @param <T> the content value type
 */
public abstract class SynchronousSource<T> implements Queue<T> {

    /**
     * Sets a Runnable instance to be called when the queue has data available for
     * peek/poll.
     * 
     * @param call the runnable to call
     */
    public abstract void onDrainable(Runnable call);
    
    /**
     * Sets a Consumer instance to be called when the queue can't produce some value for
     * peek/pool and doesn't want to throw.
     * 
     * @param errorCall the Consumer to call
     */
    public abstract void onError(Consumer<Throwable> errorCall);
    
    // -----------------------------------------------------------------------------------
    // The rest of the methods are not applicable
    // -----------------------------------------------------------------------------------
    
    @Override
    public final boolean offer(T e) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }
    
    @Override
    public final int size() {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final boolean contains(Object o) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final Iterator<T> iterator() {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final Object[] toArray() {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final <U> U[] toArray(U[] a) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final boolean add(T e) {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final T remove() {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }

    @Override
    public final T element() {
        throw new UnsupportedOperationException("Operators should not use this method!");
    }
}
