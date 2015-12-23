package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.*;

import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 */
public final class PublisherError<T> implements Publisher<T> {

    final Supplier<? extends Throwable> supplier;
    
    public PublisherError(Throwable error) {
        this(create(error));
    }
    
    static Supplier<Throwable> create(Throwable error) {
        Objects.requireNonNull(error);
        return () -> error;
    }
    
    public PublisherError(Supplier<? extends Throwable> supplier) {
        this.supplier = Objects.requireNonNull(supplier);
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Throwable e;
        
        try {
            e = supplier.get();
        } catch (Throwable ex) {
            e = ex;
        }
        
        if (e == null) {
            e = new NullPointerException("The Throwable returned by the supplier is null");
        }
        
        EmptySubscription.error(s, e);
    }
}
