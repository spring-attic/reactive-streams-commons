package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 */
public final class PublisherError<T> 
extends Px<T> {

    final Supplier<? extends Throwable> supplier;
    
    final boolean whenRequested;

    public PublisherError(Throwable error) {
        this(create(error), false);
    }

    public PublisherError(Throwable error, boolean whenRequested) {
        this(create(error), whenRequested);
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
        this(supplier, false);
    }

    
    public PublisherError(Supplier<? extends Throwable> supplier, boolean whenRequested) {
        this.supplier = Objects.requireNonNull(supplier);
        this.whenRequested = whenRequested;
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

        if (whenRequested) {
            s.onSubscribe(new PublisherErrorSubscription(s, e));
        } else {
            EmptySubscription.error(s, e);
        }
    }
    
    static final class PublisherErrorSubscription 
    implements Subscription {
        final Subscriber<?> actual;
        
        final Throwable error;
        
        volatile int once;
        static final AtomicIntegerFieldUpdater<PublisherErrorSubscription> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherErrorSubscription.class, "once");
        
        public PublisherErrorSubscription(Subscriber<?> actual, Throwable error) {
            this.actual = actual;
            this.error = error;
        }
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (ONCE.compareAndSet(this, 0, 1)) {
                    actual.onError(error);
                }
            }
        }
        @Override
        public void cancel() {
            once = 1;
        }
    }
}
