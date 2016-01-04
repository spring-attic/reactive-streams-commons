package reactivestreams.commons.publisher;

import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.support.ReactiveState;

/**
 * Represents an empty publisher which only calls onSubscribe and onComplete.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */
public final class PublisherEmpty implements Publisher<Object>, Supplier<Object>,
                                             ReactiveState.Factory,
                                             ReactiveState.ActiveUpstream {

    private static final Publisher<Object> INSTANCE = new PublisherEmpty();

    private PublisherEmpty() {
        // deliberately no op
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return true;
    }

    @Override
    public void subscribe(Subscriber<? super Object> s) {
        s.onSubscribe(EmptySubscription.INSTANCE);
        s.onComplete();
    }

    /**
     * Returns a properly parametrized instance of this empty Publisher.
     *
     * @return a properly parametrized instance of this empty Publisher
     */
    @SuppressWarnings("unchecked")
    public static <T> Publisher<T> instance() {
        return (Publisher<T>) INSTANCE;
    }

    @Override
    public Object get() {
        return null; /* Scalar optimizations on empty */
    }
}
