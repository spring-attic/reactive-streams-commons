package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.*;

import org.reactivestreams.Publisher;

/**
 * Experimental base class with fluent API.
 *
 * <p>
 * Remark: in Java 8, this could be an interface with default methods but some library
 * users need Java 7.
 * 
 * @param <T> the output value type
 */
public abstract class PublisherBase<T> implements Publisher<T> {

    static final int BUFFER_SIZE = 128;
    
    static final Supplier<Queue<Object>> QUEUE_SUPPLIER = new Supplier<Queue<Object>>() {
        @Override
        public Queue<Object> get() {
            return new ConcurrentLinkedQueue<>();
        }
    };
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static <T> Supplier<Queue<T>> queueSupplier() {
        return (Supplier)QUEUE_SUPPLIER;
    }
    
    public final <R> PublisherBase<R> map(Function<? super T, ? extends R> mapper) {
        return new PublisherMap<>(this, mapper);
    }
    
    public final <R> PublisherBase<R> switchMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return new PublisherSwitchMap<>(this, mapper, queueSupplier(), BUFFER_SIZE);
    }
}
