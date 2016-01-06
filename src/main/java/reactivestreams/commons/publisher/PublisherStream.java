package reactivestreams.commons.publisher;

import java.util.*;
import java.util.stream.Stream;

import org.reactivestreams.Subscriber;

import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.support.ReactiveState;

/**
 * Emits the contents of a Stream source.
 *
 * @param <T> the value type
 */
public final class PublisherStream<T> 
extends PublisherBase<T>
implements 
                                                 ReactiveState.Factory,
                                                 ReactiveState.Upstream {

    final Stream<? extends T> stream;

    public PublisherStream(Stream<? extends T> iterable) {
        this.stream = Objects.requireNonNull(iterable, "stream");
    }

    @Override
    public Object upstream() {
        return stream;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Iterator<? extends T> it;

        try {
            it = stream.iterator();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        PublisherIterable.subscribe(s, it);
    }

}
