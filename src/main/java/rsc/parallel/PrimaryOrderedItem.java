package rsc.parallel;

/**
 * Value container with only a primary index.
 * @param <T> the value type
 */
public final class PrimaryOrderedItem<T> implements OrderedItem<T> {

    final long index;

    T value;
    
    
    private PrimaryOrderedItem(T value, long index) {
        this.value = value;
        this.index = index;
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
        return Long.compare(index, o.index());
    }
    
    /**
     * Creates an primary OrderedItem instance.
     * @param <T> the value type
     * @param value the value to hold
     * @param index the primary index
     * @return the new instance
     */
    public static <T> OrderedItem<T> of(T value, long index) {
        return new PrimaryOrderedItem<>(value, index);
    }
}
