package rsc.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import rsc.flow.Receiver;
import rsc.subscriber.SubscriberState;

/**
 * Keep reference to the upstream Publisher in order to apply operator Subscribers
 *
 * @param <T> the upstream value type
 * @param <R> the downstream value type
 */
public abstract class PublisherSource<T, R> 
    extends Px<R>
        implements Receiver, SubscriberState {

    final protected Publisher<? extends T> source;

    public PublisherSource(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    /**
     * The upstream source
     *
     * @return the upstream source
     */
    @Override
    public final Publisher<? extends T> upstream() {
        return source;
    }
}
