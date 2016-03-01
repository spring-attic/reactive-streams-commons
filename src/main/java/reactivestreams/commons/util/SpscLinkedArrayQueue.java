package reactivestreams.commons.util;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * An unbounded, array-backed single-producer, single-consumer queue with a fixed link size.
 *
 * This implementation is based on JCTools' SPSC algorithms:
 * <a href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/SpscUnboundedArrayQueue.java'>SpscUnboundedArrayQueue</a>
 * and <a href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/atomic/SpscUnboundedAtomicArrayQueue.java'>SpscUnboundedAtomicArrayQueue</a>
 * of which the {@code SpscUnboundedAtomicArrayQueue} was contributed by one of the authors of this library. The notable difference
 * is that this class is not padded and there is no lookahead cache involved;
 * padding has a toll on short lived or bursty uses and lookahead doesn't really matter with small queues.
 * 
 * @param <T> the value type
 */
public final class SpscLinkedArrayQueue<T> extends AbstractQueue<T> {

    final int mask;
    
    volatile long producerIndex;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SpscLinkedArrayQueue> PRODUCER_INDEX =
            AtomicLongFieldUpdater.newUpdater(SpscLinkedArrayQueue.class, "producerIndex");
    AtomicReferenceArray<Object> producerArray;
    
    volatile long consumerIndex;
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<SpscLinkedArrayQueue> CONSUMER_INDEX =
            AtomicLongFieldUpdater.newUpdater(SpscLinkedArrayQueue.class, "consumerIndex");
    AtomicReferenceArray<Object> consumerArray;
    
    static final Object NEXT = new Object();
    
    public SpscLinkedArrayQueue(int linkSize) {
        int c = PowerOf2.roundUp(Math.min(2, linkSize));
        this.producerArray = this.consumerArray = new AtomicReferenceArray<>(c + 1);
        this.mask = c - 1;
    }

    @Override
    public boolean offer(T e) {
        Objects.requireNonNull(e);
        
        long pi = producerIndex;
        AtomicReferenceArray<Object> a = producerArray;
        int m = mask;
        
        int offset1 = (int)(pi + 1) & m;
        
        if (a.get(offset1) != null) {
            int offset = (int)pi & m;

            AtomicReferenceArray<Object> b = new AtomicReferenceArray<>(m + 2);
            producerArray = b;
            b.lazySet(offset, e);
            a.lazySet(m + 1, b);
            PRODUCER_INDEX.lazySet(this, pi + 1);
            a.lazySet(offset, NEXT);
        } else {
            int offset = (int)pi & m;
            PRODUCER_INDEX.lazySet(this, pi + 1);
            a.lazySet(offset, e);
        }
        
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T poll() {
        long ci = consumerIndex;
        AtomicReferenceArray<Object> a = consumerArray;
        int m = mask;

        int offset = (int)ci & m;
        
        Object o = a.get(offset);
        
        if (o == null) {
            return null;
        }
        if (o == NEXT) {
            a = (AtomicReferenceArray<Object>)a.get(m + 1);
            o = a.get(offset);
            consumerArray = a;
        }
        CONSUMER_INDEX.lazySet(this, ci + 1);
        a.lazySet(offset, null);
        
        return (T)o;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public T peek() {
        long ci = consumerIndex;
        AtomicReferenceArray<Object> a = consumerArray;
        int m = mask;

        int offset = (int)ci & m;
        
        Object o = a.get(offset);
        
        if (o == null) {
            return null;
        }
        if (o == NEXT) {
            a = (AtomicReferenceArray<Object>)a.get(m + 1);
            o = a.get(offset);
        }
        
        return (T)o;
    }
    
    @Override
    public boolean isEmpty() {
        return producerIndex == consumerIndex;
    }
    
    @Override
    public int size() {
        long ci = consumerIndex;
        for (;;) {
            long pi = producerIndex;
            long ci2 = consumerIndex;
            if (ci == ci2) {
                return (int)(pi - ci);
            }
            ci = ci2;
        }
    }
    
    @Override
    public void clear() {
        while (poll() != null && !isEmpty());
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }
}
