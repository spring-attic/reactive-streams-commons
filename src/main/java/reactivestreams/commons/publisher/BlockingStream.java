package reactivestreams.commons.publisher;

import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.reactivestreams.Publisher;
import reactivestreams.commons.util.ExceptionHelper;

/**
 * An iterable that consumes a Publisher in a blocking fashion.
 * 
 * <p> It also implements methods to stream the contents via Stream
 * that also supports cancellation.
 *
 * @param <T> the value type
 */
public final class BlockingStream<T> implements Iterable<T> {

    final Publisher<? extends T> source;

    final long batchSize;

    final Supplier<Queue<T>> queueSupplier;

    public BlockingStream(Publisher<? extends T> source, long batchSize, Supplier<Queue<T>> queueSupplier) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize > 0 required but it was " + batchSize);
        }
        this.source = Objects.requireNonNull(source, "source");
        this.batchSize = batchSize;
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
    }
    
    @Override
    public Iterator<T> iterator() {
        BlockingIterable.SubscriberIterator<T> it = createIterator();
        
        source.subscribe(it);
        
        return it;
    }

    BlockingIterable.SubscriberIterator<T> createIterator() {
        Queue<T> q;
        
        try {
            q = queueSupplier.get();
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            ExceptionHelper.fail(e);
            return null;
        }
        
        if (q == null) {
            throw new NullPointerException("The queueSupplier returned a null queue");
        }
        
        return new BlockingIterable.SubscriberIterator<>(q, batchSize);
    }
    
    @Override
    public Spliterator<T> spliterator() {
        return stream().spliterator(); // cancellation should be composed through this way
    }

	/**
     * @return
     */
    public Stream<T> stream() {
        BlockingIterable.SubscriberIterator<T> it = createIterator();
        source.subscribe(it);

        Spliterator<T> sp = Spliterators.spliteratorUnknownSize(it, 0);
        
        return StreamSupport.stream(sp, false).onClose(it);
    }

	/**
     * @return
     */
    public Stream<T> parallelStream() {
        BlockingIterable.SubscriberIterator<T> it = createIterator();
        source.subscribe(it);

        Spliterator<T> sp = Spliterators.spliteratorUnknownSize(it, 0);
        
        return StreamSupport.stream(sp, true).onClose(it);
    }
}
