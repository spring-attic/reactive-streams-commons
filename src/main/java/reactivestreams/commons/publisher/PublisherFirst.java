package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Expects and emits a single item at most from the source,
 * IndexOutOfBoundsException for a multi-item source.
 *
 * @param <T> the value type
 */
public final class PublisherFirst<T> extends PublisherSource<T, T> {

    public PublisherFirst(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherSingle.PublisherSingleSubscriber<>(s, PublisherSingle
                .completeOnEmptySequence()));
    }

}
