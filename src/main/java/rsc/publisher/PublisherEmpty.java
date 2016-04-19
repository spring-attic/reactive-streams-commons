package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.flow.Fuseable;
import rsc.state.Completable;
import rsc.util.EmptySubscription;

/**
 * Represents an empty publisher which only calls onSubscribe and onComplete.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */
public final class PublisherEmpty 
extends Px<Object>
implements Fuseable.ScalarCallable<Object>, Completable {

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
     *
     * @param <T> the output type
     * @return a properly parametrized instance of this empty Publisher
     */
    @SuppressWarnings("unchecked")
    public static <T> Px<T> instance() {
        return (Px<T>) INSTANCE;
    }

    @Override
    public Object call() {
        return null; /* Scalar optimizations on empty */
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return true;
    }
}
