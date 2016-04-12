package rsc.util;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class SpscFreeListTracker<T> {
    private volatile T[] array = empty();
    
    private int[] free = FREE_EMPTY;
    
    private long producerIndex;
    private long consumerIndex;
    
    volatile int size;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SpscFreeListTracker> SIZE =
            AtomicIntegerFieldUpdater.newUpdater(SpscFreeListTracker.class, "size");
    
    private static final int[] FREE_EMPTY = new int[0];

    protected abstract T[] empty();
    
    protected abstract T[] terminated();
    
    protected abstract T[] newArray(int size);
    
    protected abstract void unsubscribeEntry(T entry);
    
    protected abstract void setIndex(T entry, int index);

    
    protected final void unsubscribe() {
        T[] a;
        T[] t = terminated();
        synchronized (this) {
            a = array;
            if (a == t) {
                return;
            }
            SIZE.lazySet(this, 0);
            free = null;
            array = t;
        }
        for (T e : a) {
            if (e != null) {
                unsubscribeEntry(e);
            }
        }
    }
    
    public final T[] get() {
        return array;
    }
    
    public final boolean add(T entry) {
        T[] a = array;
        if (a == terminated()) {
            return false;
        }
        synchronized (this) {
            a = array;
            if (a == terminated()) {
                return false;
            }
            
            int idx = pollFree();
            if (idx < 0) {
                int n = a.length;
                T[] b = n != 0 ? newArray(n << 1) : newArray(4);
                System.arraycopy(a, 0, b, 0, n);
                
                array = b;
                a = b;

                int m = b.length;
                int[] u = new int[m];
                for (int i = n + 1; i < m; i++) {
                    u[i] = i;
                }
                free = u;
                consumerIndex = n + 1;
                producerIndex = m;
                
                idx = n;
            }
            setIndex(entry, idx);
            a[idx] = entry;
            SIZE.lazySet(this, size + 1);
            return true;
        }
    }
    
    public final void remove(int index) {
        synchronized (this) {
            T[] a = array;
            if (a != terminated()) {
                a[index] = null;
                offerFree(index);
                SIZE.lazySet(this, size - 1);
            }
        }
    }
    
    private int pollFree() {
        int[] a = free;
        int m = a.length - 1;
        long ci = consumerIndex;
        if (producerIndex == ci) {
            return -1;
        }
        int offset = (int)ci & m;
        consumerIndex = ci + 1;
        return a[offset];
    }
    
    private void offerFree(int index) {
        int[] a = free;
        int m = a.length - 1;
        long pi = producerIndex;
        int offset = (int)pi & m;
        a[offset] = index;
        producerIndex = pi + 1;
    }
    
    protected final boolean isEmpty() {
        return size == 0;
    }
}
