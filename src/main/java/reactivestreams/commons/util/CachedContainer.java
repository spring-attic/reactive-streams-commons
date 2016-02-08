package reactivestreams.commons.util;

/**
 * A container of Ts that uses a backing HashSet for O(1) add/remove and
 * a cached flat array of all active items.
 *
 * @param <T> the contained type
 */
public abstract class CachedContainer<T> {

    protected volatile T[] array;
    
    protected int cachedCount;
    
    protected OpenHashSet<T> set;
    
    protected volatile long version;
    
    protected long cachedVersion;
    
    public final void init() {
        array = newArray(0);
    }

    /**
     * Retrieves a snapshot array of the contained objects.
     * <p>Should be called only by the drain loop.
     * @return a snapshot array of the contained objects
     */
    public final T[] get() {
        if (cachedVersion == version) {
            return array;
        }
        synchronized (this) {
            T[] a = array;
            if (a == terminatedArray()) {
                cachedCount = 0;
                return a;
            }
            OpenHashSet<T> s = set;
            T[] k = s.keys;
            int m = k.length;
            int n = a.length;
            int c = s.size;
            
            if (n < c || c <= (m >> 2)) {
                a = newArray(PowerOf2.roundUp(c));
            }
            int j = 0;
            for (int i = 0; i < m; i++) {
                T e = k[i];
                if (e != null) {
                    a[j++] = e;
                }
            }
            for (int i = j; i < a.length; i++) {
                a[i] = null;
            }
            
            cachedCount = j;
            cachedVersion = version;
            array = a;
            
            return a;
        }
    }

    /**
     * Adds an item to the container.
     * <p>Should be called only by the producer 
     * @param item the item to add
     * @return true if successful, false if the container is terminated
     */
    public final boolean add(T item) {
        T[] term = terminatedArray();
        if (array == term) {
            return false;
        }
        synchronized (this) {
            T[] a = array;
            if (a == term) {
                return false;
            }
            
            OpenHashSet<T> s = set;
            if (s == null) {
                s = new OpenHashSet<>(4);
                set = s;
            } else {
                int n = s.size;
                if (n <= (s.keys.length >> 2)) {
                    OpenHashSet<T> t = new OpenHashSet<>(PowerOf2.roundUp(n << 1));
                    s.forEach(t::add);
                    set = t;
                    s = t;
                }
            }
            s.add(item);
            version++;
        }
        return true;
    }

    /**
     * Remove the item from the container.
     * <p>Should be called from the drain loop.
     * <p>The index should be a valid index referencing the array returned by get().
     * @param item the item to remove
     * @param index the index to null out
     */
    public final void remove(T item, int index) {
        T[] a = array;
        T[] term = terminatedArray();
        if (a == term) {
            return;
        }

        --cachedCount;
        a[index] = null;
        
        synchronized (this) {
            a = array;
            if (a == term) {
                return;
            }
            
            OpenHashSet<T> s = set;
            s.remove(item);

            int n = s.size;
            if (n <= (s.keys.length >> 2)) {
                OpenHashSet<T> t = new OpenHashSet<>(PowerOf2.roundUp(n << 1));
                s.forEach(t::add);
                set = t;
                cachedVersion--;
            }
        }
    }

    /**
     * Should create an item array with the given arbitrary size.
     * @param size the size
     * @return the array
     */
    protected abstract T[] newArray(int size);
    
    /**
     * Returns the terminated indicator array.
     * @return the terminated indicator array
     */
    protected abstract T[] terminatedArray();

    /**
     * Atomically terminates the container.
     * @return the last content before the termination
     */
    public final T[] terminate() {
        T[] term = terminatedArray();
        if (array == term) {
            return term;
        }
        T[] a;
        synchronized (this) {
            if (array == term) {
                return term;
            }
            
            OpenHashSet<T> s = set;
            if (s == null) {
                array = term;
                version = Long.MAX_VALUE;
                return newArray(0);
            }
            a = set.keys;
            set = null;
            array = term;
            version = Long.MAX_VALUE;
        }
        // correct the array type
        int n = a.length;
        T[] b = newArray(n);
        System.arraycopy(a, 0, b, 0, n);
        return b;
    }
}
