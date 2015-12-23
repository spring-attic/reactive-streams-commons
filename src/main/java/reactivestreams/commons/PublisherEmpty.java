package reactivestreams.commons;

import org.reactivestreams.*;

import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Represents an empty publisher which only calls onSubscribe and onComplete.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */
public final class PublisherEmpty implements Publisher<Object> {

    private static final Publisher<Object> INSTANCE = new PublisherEmpty();
    
    private PublisherEmpty() {
        // deliberately no op
    }
    
    @Override
    public void subscribe(Subscriber<? super Object> s) {
        s.onSubscribe(EmptySubscription.INSTANCE);
        s.onComplete();
    }
    
    /**
     * Returns a properly parametrized instance of this empty Publisher.
     * @return a properly parametrized instance of this empty Publisher
     */
    @SuppressWarnings("unchecked")
    public static <T> Publisher<T> instance() {
        return (Publisher<T>)INSTANCE;
    }
}
