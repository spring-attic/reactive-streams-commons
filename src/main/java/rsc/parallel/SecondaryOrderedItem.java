package rsc.parallel;

/**
 * Value container with primary and secondary indexes.
 * <p>
 * Useful when an indexed primary item generates sub-items in sequence
 * and one wants to keep the order of the primary and secondary items.
 * 
 * @param <T> the value type
 */
public final class SecondaryOrderedItem<T> implements OrderedItem<T> {
    
    final long index;
    
    final long subIndex;


    T value;

    private SecondaryOrderedItem(T value, long index, long subIndex) {
        this.value = value;
        this.index = index;
        this.subIndex = subIndex;
    }
    
    @Override
    public T get() {
        return value;
    }

    @Override
    public void set(T newValue) {
        this.value = newValue;
    }

    @Override
    public long index() {
        return index;
    }
    
    @Override
    public int compareTo(OrderedItem<T> o) {
        int c = Long.compare(index, o.index());
        if (c == 0) {
            if (o instanceof SecondaryOrderedItem) {
                return Long.compare(subIndex, ((SecondaryOrderedItem<T>)o).subIndex);
            }
        }
        return c;
    }
    
    /**
     * Creates an secondary OrderedItem instance.
     * @param <T> the value type
     * @param value the value to hold
     * @param index the primary index
     * @param subIndex the secondary index
     * @return the new instance
     */
    public static <T> OrderedItem<T> of(T value, long index, long subIndex) {
        return new SecondaryOrderedItem<>(value, index, subIndex);
    }
    
    @Override
    public <U> OrderedItem<U> copy(U u) {
        return of(u, index, subIndex);
    }
}
