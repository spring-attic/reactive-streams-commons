package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ReactiveState;

/**
 * Defers the creation of the actual Publisher the Subscriber will be subscribed to.
 *
 * @param <T> the value type
 */
public final class PublisherDefer<T> 
extends PublisherBase<T>
implements 
                                                ReactiveState.Factory,
                                                ReactiveState.Upstream {

    final Supplier<? extends Publisher<? extends T>> supplier;

    public PublisherDefer(Supplier<? extends Publisher<? extends T>> supplier) {
        this.supplier = Objects.requireNonNull(supplier, "supplier");
    }

    @Override
    public Object upstream() {
        return supplier;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T> p;

        try {
            p = supplier.get();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        if (p == null) {
            EmptySubscription.error(s, new NullPointerException("The Producer returned by the supplier is null"));
            return;
        }

        p.subscribe(s);
    }
}
