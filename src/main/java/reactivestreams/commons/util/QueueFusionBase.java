package reactivestreams.commons.util;

import java.util.*;

/**
 * Abstract base class for queue-fusion based optimizations.
 *
 * @param <T> the value type emitted
 */
abstract class QueueFusionBase<T> implements Queue<T> {
    
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
