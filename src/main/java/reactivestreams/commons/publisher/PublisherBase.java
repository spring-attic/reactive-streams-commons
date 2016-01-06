package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.*;

import org.reactivestreams.*;

/**
 * Experimental base class with fluent API.
 *
 * <p>
 * Remark: in Java 8, this could be an interface with default methods but some library
 * users need Java 7.
 * 
 * <p>
 * Use {@link #wrap(Publisher)} to wrap any Publisher. 
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
    
    public final PublisherBase<T> filter(Predicate<? super T> predicate) {
        return new PublisherFilter<>(this, predicate);
    }
    
    public final PublisherBase<T> take(long n) {
        return new PublisherTake<>(this, n);
    }
    
    public final PublisherBase<T> concatWith(Publisher<? extends T> other) {
        return new PublisherConcatArray<>(this, other);
    }
    
    public final PublisherBase<T> ambWith(Publisher<? extends T> other) {
        return new PublisherAmb<>(this, other);
    }
    
    public final <U, R> PublisherBase<R> withLatestFrom(Publisher<? extends U> other, BiFunction<? super T, ? super U, ? extends R> combiner) {
        return new PublisherWithLatestFrom<>(this, other, combiner);
    }
    
    public final <R> PublisherBase<R> switchMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
        return new PublisherSwitchMap<>(this, mapper, queueSupplier(), BUFFER_SIZE);
    }
    
    public final PublisherBase<T> retryWhen(Function<? super PublisherBase<Throwable>, ? extends Publisher<? extends Object>> whenFunction) {
        return new PublisherRetryWhen<>(this, whenFunction);
    }

    public final PublisherBase<T> repeatWhen(Function<? super PublisherBase<Object>, ? extends Publisher<? extends Object>> whenFunction) {
        return new PublisherRepeatWhen<>(this, whenFunction);
    }

    public final <U> PublisherBase<List<T>> buffer(Publisher<U> other) {
        return buffer(other, () -> new ArrayList<>());
    }
    
    public final <U, C extends Collection<? super T>> PublisherBase<C> buffer(Publisher<U> other, Supplier<C> bufferSupplier) {
        return new PublisherBufferBoundary<>(this, other, bufferSupplier);
    }
    
    static final class PublisherBaseWrapper<T> extends PublisherSource<T, T> {
        public PublisherBaseWrapper(Publisher<? extends T> source) {
            super(source);
        }
        
        @Override
        public void subscribe(Subscriber<? super T> s) {
            source.subscribe(s);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static <T> PublisherBase<T> wrap(Publisher<? extends T> source) {
        if (source instanceof PublisherBase) {
            return (PublisherBase<T>)source;
        }
        return new PublisherBaseWrapper<>(source);
    }
}
