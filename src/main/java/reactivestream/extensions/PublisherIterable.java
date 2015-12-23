package reactivestream.extensions;

import java.util.*;

import org.reactivestreams.*;

import reactivestream.extensions.internal.subscriptions.EmptySubscription;

/**
 * Emits the contents of an Iterable source.
 *
 * @param <T> the value type
 */
public final class PublisherIterable<T> implements Publisher<T> {

    final Iterable<? extends T> iterable;
    
    public PublisherIterable(Iterable<? extends T> iterable) {
        this.iterable = Objects.requireNonNull(iterable, "iterable");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Iterator<? extends T> it;
        
        try {
            it = iterable.iterator();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        PublisherStream.subscribe(s, it);
    }
}
