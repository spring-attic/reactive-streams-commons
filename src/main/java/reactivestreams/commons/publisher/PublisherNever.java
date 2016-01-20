package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ReactiveState;

/**
 * Represents an never publisher which only calls onSubscribe.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */
public final class PublisherNever 
extends PublisherBase<Object>
implements 
                                             ReactiveState.Factory,
                                             ReactiveState.ActiveUpstream {

    private static final Publisher<Object> INSTANCE = new PublisherNever();

    private PublisherNever() {
        // deliberately no op
    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public void subscribe(Subscriber<? super Object> s) {
        s.onSubscribe(EmptySubscription.INSTANCE);
    }

    /**
     * Returns a properly parametrized instance of this never Publisher.
     *
     * @param <T> the value type
     * @return a properly parametrized instance of this never Publisher
     */
    @SuppressWarnings("unchecked")
    public static <T> PublisherBase<T> instance() {
        return (PublisherBase<T>) INSTANCE;
    }
}
