package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import rsc.flow.Receiver;
import rsc.subscriber.DeferredScalarSubscriber;
import rsc.util.ExceptionHelper;

/**
 * Executes a Callable function and emits a single value to each individual Subscriber.
 * <p>
 *  Preferred to {@link Supplier} because the Callable may throw.
 *
 * @param <T> the returned value type
 */
public final class PublisherCallable<T> 
extends Px<T>
        implements Receiver, Callable<T> {

    final Callable<? extends T> callable;

    public PublisherCallable(Callable<? extends T> callable) {
        this.callable = Objects.requireNonNull(callable, "callable");
    }

    @Override
    public Object upstream() {
        return callable;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        DeferredScalarSubscriber<T, T> sds = new DeferredScalarSubscriber<>(s);

        s.onSubscribe(sds);

        if (sds.isCancelled()) {
            return;
        }

        T t;
        try {
            t = callable.call();
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            s.onError(ExceptionHelper.unwrap(e));
            return;
        }

        if (t == null) {
            s.onError(new NullPointerException("The callable returned null"));
            return;
        }

        sds.complete(t);
    }
    
    @Override
    public T call() throws Exception {
        return callable.call();
    }
}
