package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.internal.subscription.EmptySubscription;

/**
 * Keep reference to the upstream Publisher in order to apply operator Subscribers
 *
 * @param <T> the upstream value type
 * @param <R> the downstream value type
 */
public class PublisherSource<T, R> implements Publisher<R> {

    final Publisher<? extends T> source;

    public PublisherSource(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
    }
    
    public final Publisher<? extends T> upstream() {
        return source;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void subscribe(Subscriber<? super R> s) {
        source.subscribe((Subscriber<? super T>) s);
    }
}
