package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;

import java.util.Objects;

/**
 * Keep reference to the upstream Publisher in order to apply operator Subscribers
 *
 * @param <T> the upstream value type
 * @param <R> the downstream value type
 */
public abstract class PublisherSource<T, R> implements Publisher<R> {

    final protected Publisher<? extends T> source;

    public PublisherSource(Publisher<? extends T> source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    /**
     * The upstream source
     *
     * @return
     */
    public final Publisher<? extends T> upstream() {
        return source;
    }

}
