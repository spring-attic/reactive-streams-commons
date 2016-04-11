package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Merges a fixed array of Publishers.
 * @param <T> the element type of the publishers
 */
public final class PublisherMerge<T> extends Px<T> {

    final Publisher<? extends T>[] sources;
    
    final boolean delayError;
    
    final int maxConcurrency;
    
    final Supplier<? extends Queue<T>> mainQueueSupplier;

    final int prefetch;
    
    final Supplier<? extends Queue<T>> innerQueueSupplier;
    
    public PublisherMerge(Publisher<? extends T>[] sources,
            boolean delayError, int maxConcurrency, 
            Supplier<? extends Queue<T>> mainQueueSupplier, 
                    int prefetch, Supplier<? extends Queue<T>> innerQueueSupplier) {
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency > 0 required but it was " + maxConcurrency);
        }
        this.sources = Objects.requireNonNull(sources, "sources");
        this.delayError = delayError;
        this.maxConcurrency = maxConcurrency;
        this.prefetch = prefetch;
        this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
        this.innerQueueSupplier = Objects.requireNonNull(innerQueueSupplier, "innerQueueSupplier");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        @SuppressWarnings("unchecked")
        PublisherFlatMap.PublisherFlatMapMain<Publisher<? extends T>, T> merger = new PublisherFlatMap.PublisherFlatMapMain<>(
                s, Px.IDENTITY_FUNCTION, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier);
        
        merger.onSubscribe(new PublisherArray.ArraySubscription<>(merger, sources));
    }
    
    /**
     * Returns a new instance which has the additional source to be merged together with
     * the current array of sources.
     * <p>
     * This operation doesn't change the current PublisherMerge instance.
     * 
     * @param source the new source to merge with the others
     * @return the new PublisherMerge instance
     */
    public PublisherMerge<T> mergeAdditionalSource(Publisher<? extends T> source) {
        int n = sources.length;
        @SuppressWarnings("unchecked")
        Publisher<? extends T>[] newArray = new Publisher[n + 1];
        System.arraycopy(sources, 0, newArray, 0, n);
        newArray[n] = source;
        
        // increase the maxConcurrency because if merged separately, it would have run concurrently anyway
        int mc = maxConcurrency;
        if (mc != Integer.MAX_VALUE) {
            mc++;
        }
        
        return new PublisherMerge<>(newArray, delayError, mc, mainQueueSupplier, prefetch, innerQueueSupplier);
    }
}
