package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.internal.ScalarDelayedArbiter;

/**
 * Emits the value or error produced by the wrapped CompletableFuture.
 * <p>
 * Note that if Subscribers cancel their subscriptions, the CompletableFuture
 * is not cancelled.
 *
 * @param <T> the value type
 */
public final class PublisherCompletableFuture<T> implements Publisher<T> {

    final CompletableFuture<? extends T> future;
    
    public PublisherCompletableFuture(CompletableFuture<? extends T> future) {
        this.future = Objects.requireNonNull(future, "future");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        ScalarDelayedArbiter<T> sds = new ScalarDelayedArbiter<>(s);
        
        s.onSubscribe(sds);
        
        if (sds.isCancelled()) {
            return;
        }
        
        future.whenComplete((v, e) -> {
            if (e != null) {
                s.onError(e);
            } else 
            if (v != null) {
                sds.set(v);
            } else {
                s.onError(new NullPointerException("The future produced a null value"));
            }
        });
    }
}
