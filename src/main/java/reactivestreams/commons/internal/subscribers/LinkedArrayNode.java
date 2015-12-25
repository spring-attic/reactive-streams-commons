package reactivestreams.commons.internal.subscribers;

/**
 * Node in a linked array list that is only appended.
 *
 * @param <T> the value type
 */
public final class LinkedArrayNode<T> {
    
    static final int DEFAULT_CAPACITY = 16;
    
    final T[] array;
    int count;
    
    LinkedArrayNode<T> next;
    
    @SuppressWarnings("unchecked")
    public LinkedArrayNode(T value) {
        array = (T[])new Object[DEFAULT_CAPACITY];
        array[0] = value;
        count = 1;
    }
}