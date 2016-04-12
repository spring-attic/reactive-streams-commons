package rsc.util;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class SpscSetTracker<T> {
    final float loadFactor;
    int mask;
    volatile int size;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SpscSetTracker> SIZE =
            AtomicIntegerFieldUpdater.newUpdater(SpscSetTracker.class, "size");
    
    int maxSize;
    volatile T[] keys;
    
    public SpscSetTracker() {
        this.loadFactor = 0.75f;
        int c = 16;
        this.mask = c - 1;
        this.maxSize = (int)(loadFactor * c);
        this.keys = newArray(c);
    }
    
    public boolean add(T value) {
        T[] a = keys;
        
        final T[] t = terminated();
        if (a == t) {
            return false;
        }
        
        synchronized (this) {
            a = keys;
            if (a == t) {
                return false;
            }
            final int m = mask;
            
            int pos = mix(value.hashCode()) & m;
            T curr = a[pos];
            if (curr != null) {
                if (curr.equals(value)) {
                    return false;
                }
                for (;;) {
                    pos = (pos + 1) & m;
                    curr = a[pos];
                    if (curr == null) {
                        break;
                    }
                    if (curr.equals(value)) {
                        return false;
                    }
                }
            }
            a[pos] = value;
            int s = size + 1;
            SIZE.lazySet(this, s);
            if (s >= maxSize) {
                rehash();
            }
            return true;
        }
    }
    public boolean remove(T value) {
        T[] a = keys;
        T[] t = terminated();
        
        if (a == t) {
            return false;
        }
        
        synchronized (this) {
            a = keys;
            if (a == t || size == 0) {
                return false;
            }
            int m = mask;
            int pos = mix(value.hashCode()) & m;
            T curr = a[pos];
            if (curr == null) {
                return false;
            }
            if (curr.equals(value)) {
                return removeEntry(pos, a, m);
            }
            for (;;) {
                pos = (pos + 1) & m;
                curr = a[pos];
                if (curr == null) {
                    return false;
                }
                if (curr.equals(value)) {
                    return removeEntry(pos, a, m);
                }
            }
        }
    }
    
    boolean removeEntry(int pos, T[] a, int m) {
        SIZE.lazySet(this, size - 1);
        
        int last;
        int slot;
        T curr;
        for (;;) {
            last = pos;
            pos = (pos + 1) & m;
            for (;;) {
                curr = a[pos];
                if (curr == null) {
                    a[last] = null;
                    return true;
                }
                slot = mix(curr.hashCode()) & m;
                
                if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
                    break;
                }
                
                pos = (pos + 1) & m;
            }
            a[last] = curr;
        }
    }
    
    void rehash() {
        T[] a = keys;
        int i = a.length;
        int newCap = i << 1;
        int m = newCap - 1;
        
        T[] b = newArray(newCap);
        
        
        for (int j = size; j-- != 0; ) {
            while (a[--i] == null);
            int pos = mix(a[i].hashCode()) & m;
            if (b[pos] != null) {
                for (;;) {
                    pos = (pos + 1) & m;
                    if (b[pos] == null) {
                        break;
                    }
                }
            }
            b[pos] = a[i];
        }
        
        this.mask = m;
        this.maxSize = (int)(newCap * loadFactor);
        this.keys = b;
    }
    
    private static final int INT_PHI = 0x9E3779B9;
    
    static int mix(int x) {
        final int h = x * INT_PHI;
        return h ^ (h >>> 16);
    }
    
    protected abstract T[] terminated();
    
    protected abstract T[] newArray(int size);
    
    protected abstract void unsubscribeEntry(T entry);
    
    protected final void unsubscribe() {
        T[] a;
        T[] t = terminated();
        synchronized (this) {
            a = keys;
            if (a == t) {
                return;
            }
            SIZE.lazySet(this, 0);
            keys = t;
        }
        for (T e : a) {
            if (e != null) {
                unsubscribeEntry(e);
            }
        }
    }
    
    public final T[] get() {
        return keys;
    }
    
    protected final boolean isEmpty() {
        return size == 0;
    }
}
