package reactivestreams.commons.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A bounded, array backed, single-producer single-consumer queue.
 * 
 * @param <T> the value type
 */
public final class SpscArrayQueue<T> extends AtomicReferenceArray<T> implements Queue<T> {
    /** */
    private static final long serialVersionUID = 494623116936946976L;

    final AtomicLong producerIndex;
    
    final AtomicLong consumerIndex;
    
    final int mask;
    
    public SpscArrayQueue(int capacity) {
        super(PowerOf2.roundUp(capacity));
        producerIndex = new AtomicLong();
        consumerIndex = new AtomicLong();
        mask = length() - 1;
    }
    
    @Override
    public boolean offer(T e) {
        Objects.requireNonNull(e, "e");
        long pi = producerIndex.get();
        int offset = (int)pi & mask;
        if (get(offset) != null) {
            return false;
        }
        producerIndex.lazySet(pi + 1);
        lazySet(offset, e);
        return true;
    }
    
    @Override
    public T poll() {
        long ci = consumerIndex.get();
        int offset = (int)ci & mask;
        
        T v = get(offset);
        if (v != null) {
            consumerIndex.lazySet(ci + 1);
            lazySet(offset, null);
        }
        return v;
    }
    
    @Override
    public T peek() {
        int offset = (int)consumerIndex.get() & mask;
        return get(offset);
    }
    
    @Override
    public boolean isEmpty() {
        return producerIndex.get() == consumerIndex.get();
    }
    
    @Override
    public void clear() {
        while (poll() != null && !isEmpty());
    }

    @Override
    public int size() {
        long ci = consumerIndex.get();
        for (;;) {
            long pi = producerIndex.get();
            long ci2 = consumerIndex.get();
            if (ci == ci2) {
                return (int)(pi - ci);
            }
            ci = ci2;
        }
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> R[] toArray(R[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T element() {
        throw new UnsupportedOperationException();
    }
}
