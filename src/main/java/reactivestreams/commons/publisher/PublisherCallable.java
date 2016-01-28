package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import reactivestreams.commons.flow.Receiver;
import reactivestreams.commons.subscriber.SubscriberDeferredScalar;
import reactivestreams.commons.util.ExceptionHelper;

/**
 * Executes a Callable function and emits a single value to each individual Subscriber.
 * <p>
 *  Preferred to {@link Supplier} because the Callable may throw.
 *
 * @param <T> the returned value type
 */
public final class PublisherCallable<T> 
extends PublisherBase<T>
        implements Receiver, Supplier<T> {

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

        SubscriberDeferredScalar<T, T> sds = new SubscriberDeferredScalar<>(s);

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
    public T get() {
        try {
            return callable.call();
        } catch (Throwable e) {
            throw ExceptionHelper.propagate(e);
        }
    }
}
