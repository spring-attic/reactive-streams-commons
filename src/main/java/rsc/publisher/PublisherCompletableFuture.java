package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Subscriber;

import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;

/**
 * Emits the value or error produced by the wrapped CompletableFuture.
 * <p>
 * Note that if Subscribers cancel their subscriptions, the CompletableFuture
 * is not cancelled.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.NOT_APPLICABLE, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NOT_APPLICABLE }, output = { FusionMode.ASYNC })
public final class PublisherCompletableFuture<T> 
extends Px<T>
        implements Receiver, Fuseable {

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
        DeferredScalarSubscriber<T, T> sds = new DeferredScalarSubscriber<>(s);

        s.onSubscribe(sds);

        if (sds.isCancelled()) {
            return;
        }

        future.whenComplete((v, e) -> {
            if (e != null) {
                s.onError(e);
            } else if (v != null) {
                sds.complete(v);
            } else {
                s.onError(new NullPointerException("The future produced a null value"));
            }
        });
    }
}
