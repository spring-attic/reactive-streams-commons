package reactivestreams.commons.subscriber;

import java.util.Objects;

import org.reactivestreams.*;

import reactivestreams.commons.support.ReactiveState;

/**
 * Arbitrates the requests and cancellation for a Subscription that may be set onSubscribe once only.
 * <p>
 * Note that {@link #request(long)} doesn't validate the amount.
 * 
 * @param <I> the input value type
 * @param <O> the output value type
 */
public class SubscriberDeferSubscription<I, O> 
extends SubscriberDeferSubscriptionBase<I>
implements Subscription, Subscriber<I>,
                                                          ReactiveState.DownstreamDemand,
                                                          ReactiveState.ActiveUpstream,
                                                          ReactiveState.ActiveDownstream,
                                                          ReactiveState.Downstream,
                                                          ReactiveState.Upstream {

    protected final Subscriber<? super O> subscriber;

    /**
     * Constructs a SingleSubscriptionArbiter with zero initial request.
     * 
     * @param subscriber the actual subscriber
     */
    public SubscriberDeferSubscription(Subscriber<? super O> subscriber) {
        this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
    }

    /**
     * Constructs a SingleSubscriptionArbiter with the specified initial request amount.
     *
     * @param subscriber the actual subscriber
     * @param initialRequest
     * @throws IllegalArgumentException if initialRequest is negative
     */
    public SubscriberDeferSubscription(Subscriber<? super O> subscriber, long initialRequest) {
        if (initialRequest < 0) {
            throw new IllegalArgumentException("initialRequest >= required but it was " + initialRequest);
        }
        this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
        setInitialRequest(initialRequest);
    }

    /**
     * Returns true if a subscription has been set or the arbiter has been cancelled.
     * <p>
     * Use {@link #isCancelled()} to distinguish between the two states.
     *
     * @return true if a subscription has been set or the arbiter has been cancelled
     */
    @Override
    public final boolean isStarted() {
        return s != null;
    }

    @Override
    public final boolean isTerminated() {
        return isCancelled();
    }

    @Override
    public final Subscriber<? super O> downstream() {
        return subscriber;
    }

    @Override
    public long requestedFromDownstream() {
        return requested;
    }

    @Override
    public Subscription upstream() {
        return s;
    }

    @Override
    public void onSubscribe(Subscription s) {
        set(s);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onNext(I t) {
        subscriber.onNext((O) t);
    }

    @Override
    public void onError(Throwable t) {
        subscriber.onError(t);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }
}
