package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.internal.subscriptions.ScalarDelayedSubscription;

/**
 * Executes a Callable function and emits a single value to each individual Subscriber.
 * <p>
 * TODO can't really implement {@link Supplier} because the Callable may throw.
 * 
 * @param <T> the returned value type
 */
public final class PublisherCallable<T> implements Publisher<T> {

    final Callable<? extends T> callable;
    
    public PublisherCallable(Callable<? extends T> callable) {
        this.callable = Objects.requireNonNull(callable, "callable");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        
        ScalarDelayedSubscription<T> sds = new ScalarDelayedSubscription<>(s);
        
        s.onSubscribe(sds);
        
        if (sds.isCancelled()) {
            return;
        }
        
        T t;
        try {
            t = callable.call();
        } catch (Throwable e) {
            s.onError(e);
            return;
        }
        
        if (t == null) {
            s.onError(new NullPointerException("The callable returned null"));
            return;
        }
        
        sds.set(t);
    }
}
