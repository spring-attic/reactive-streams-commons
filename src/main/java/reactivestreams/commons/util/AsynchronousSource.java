package reactivestreams.commons.util;

import java.util.*;

import org.reactivestreams.Subscription;

/**
 * Base class for asynchronous sources which can act as a queue and subscription
 * at the same time, saving on allocating another queue most of the time.
 * 
 * <p>
 * Implementation note: even though it looks exactly like SynchronousSource, this
 * class has to be separate because the protocol is different (i.e., SynchronousSource should
 * be never requested).
 * 
 * @param <T> the content value type
 */
public abstract class AsynchronousSource<T> implements Queue<T>, Subscription {

    /**
     * Consumers of an AsynchronousSource have to signal it to switch to a fused-mode
     * so it no longer run its own drain loop but directly signals onNext(null) to 
     * indicate there is an item available in this queue-view.
     * <p>
     * The method has to be called while the parent is in onSubscribe and before any
     * other interaction with the Subscription.
     */
    public abstract void enableOperatorFusion();
    
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
