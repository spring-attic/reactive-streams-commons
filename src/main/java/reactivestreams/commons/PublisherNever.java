package reactivestreams.commons;

import org.reactivestreams.*;

import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Represents an never publisher which only calls onSubscribe.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */
public final class PublisherNever implements Publisher<Object> {

    private static final Publisher<Object> INSTANCE = new PublisherNever();
    
    private PublisherNever() {
        // deliberately no op
    }
    
    @Override
    public void subscribe(Subscriber<? super Object> s) {
        s.onSubscribe(EmptySubscription.INSTANCE);
    }
    
    /**
     * Returns a properly parametrized instance of this never Publisher.
     * @return a properly parametrized instance of this never Publisher
     */
    @SuppressWarnings("unchecked")
    public static <T> Publisher<T> instance() {
        return (Publisher<T>)INSTANCE;
    }
}
