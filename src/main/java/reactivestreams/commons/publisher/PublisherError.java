package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import reactivestreams.commons.state.Failurable;
import reactivestreams.commons.util.EmptySubscription;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 */
public final class PublisherError<T> 
extends PublisherBase<T>
        implements Failurable {

    final Supplier<? extends Throwable> supplier;

    public PublisherError(Throwable error) {
        this(create(error));
    }

    static Supplier<Throwable> create(final Throwable error) {
        Objects.requireNonNull(error);
        return new Supplier<Throwable>() {
            @Override
            public Throwable get() {
                return error;
            }
        };
    }

    public PublisherError(Supplier<? extends Throwable> supplier) {
        this.supplier = Objects.requireNonNull(supplier);
    }

    @Override
    public Throwable getError() {
        return supplier.get();
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Throwable e;

        try {
            e = supplier.get();
        } catch (Throwable ex) {
            e = ex;
        }

        if (e == null) {
            e = new NullPointerException("The Throwable returned by the supplier is null");
        }

        EmptySubscription.error(s, e);
    }
}
