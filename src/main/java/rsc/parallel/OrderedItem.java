package rsc.parallel;

/**
 * Represents a value with ordering information.
 * 
 * @param <T> the value type
 */
public interface OrderedItem<T> extends Comparable<OrderedItem<T>> {
    /**
     * Returns the contained value.
     * @return the contained value
     */
    T get();
    
    /**
     * Update the value in-place;
     * @param newValue
     */
    void set(T newValue);
    
    /**
     * Returns the primary index.
     * @return the primary index
     */
    long index();
    
    /**
     * Creates a new OrderedItem instance with a new value but
     * the same indexes as this instance.
     * 
     * @param <U> the new value type
     * @param u the new value
     * @return the new OrderedItem instance
     */
    <U> OrderedItem<U> copy(U u);
}
