package reactivestreams.commons.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.flow.Receiver;

public final class ScalarSubscription<T> implements Subscription, Producer, Receiver {

    /**
     * Checks if the source is a Supplier and if the mapper's publisher output is also
     * a supplier, thus avoiding subscribing to any of them.
     *
     * @param <T> the the upstream value type
     * @param <R> the downstream value type
     * @param source the source publisher
     * @param s the end consumer
     * @param mapper the mapper function
     * @return true if the optimization worked
     */
    @SuppressWarnings("unchecked")
    public static <T, R> boolean trySubscribeScalarMap(Publisher<? extends T> source, Subscriber<? super R> s, Function<?
            super T, ? extends Publisher<? extends R>> mapper) {
        if (source instanceof Supplier) {
            T t;

            try {
                t = ((Supplier<? extends T>)source).get();
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                EmptySubscription.error(s, ExceptionHelper.unwrap(e));
                return true;
            }

            if (t == null) {
                EmptySubscription.complete(s);
                return true;
            }

            Publisher<? extends R> p;

            try {
                p = mapper.apply(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                EmptySubscription.error(s, ExceptionHelper.unwrap(e));
                return true;
            }

            if (p == null) {
                EmptySubscription.error(s, new NullPointerException("The mapper returned a null Publisher"));
                return true;
            }

            if (p instanceof Supplier) {
                R v;

                try {
                    v = ((Supplier<R>)p).get();
                } catch (Throwable e) {
                    ExceptionHelper.throwIfFatal(e);
                    EmptySubscription.error(s, ExceptionHelper.unwrap(e));
                    return true;
                }

                if (v != null) {
                    s.onSubscribe(new ScalarSubscription<>(s, v));
                } else {
                    EmptySubscription.complete(s);
                }
            } else {
                p.subscribe(s);
            }

            return true;
        }

        return false;
    }

    final Subscriber<? super T> actual;

    final T value;

    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<ScalarSubscription> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(ScalarSubscription.class, "once");

    public ScalarSubscription(Subscriber<? super T> actual, T value) {
        this.value = Objects.requireNonNull(value, "value");
        this.actual = Objects.requireNonNull(actual, "actual");
    }

    @Override
    public final Subscriber<? super T> downstream() {
        return actual;
    }

    @Override
    public void request(long n) {
        if (SubscriptionHelper.validate(n)) {
            if (ONCE.compareAndSet(this, 0, 1)) {
                Subscriber<? super T> a = actual;
                a.onNext(value);
                a.onComplete();
            }
        }
    }

    @Override
    public void cancel() {
        ONCE.lazySet(this, 1);
    }

    @Override
    public Object upstream() {
        return value;
    }
}
