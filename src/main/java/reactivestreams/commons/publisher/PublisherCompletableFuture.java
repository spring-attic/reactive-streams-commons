package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.subscriber.SubscriberDeferScalar;
import reactivestreams.commons.support.ReactiveState;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Emits the value or error produced by the wrapped CompletableFuture.
 * <p>
 * Note that if Subscribers cancel their subscriptions, the CompletableFuture
 * is not cancelled.
 *
 * @param <T> the value type
 */
public final class PublisherCompletableFuture<T> 
extends PublisherBase<T>
implements Publisher<T>,
                                                            ReactiveState.Factory,
                                                            ReactiveState.Upstream{

    final CompletableFuture<? extends T> future;

    public PublisherCompletableFuture(CompletableFuture<? extends T> future) {
        this.future = Objects.requireNonNull(future, "future");
    }

    @Override
    public Object upstream() {
        return future;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        SubscriberDeferScalar<T, T> sds = new SubscriberDeferScalar<>(s);

        s.onSubscribe(sds);

        if (sds.isCancelled()) {
            return;
        }

        future.whenComplete((v, e) -> {
            if (e != null) {
                s.onError(e);
            } else if (v != null) {
                sds.set(v);
            } else {
                s.onError(new NullPointerException("The future produced a null value"));
            }
        });
    }
}
