package rsc.publisher;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;
import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Expects and emits a single item from the source or signals
 * NoSuchElementException(or a default generated value) for empty source,
 * IndexOutOfBoundsException for a multi-item source.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherSingle<T> extends PublisherSource<T, T> implements Fuseable {

    private static final Supplier<Object> COMPLETE_ON_EMPTY_SEQUENCE = new Supplier<Object>() {
        @Override
        public Object get() {
            return null; // Purposedly leave noop
        }
    };

    @Override
    public long getPrefetch() {
        return Long.MAX_VALUE;
    }

	/**
     * @param <T>
     * @return a Supplier instance marker that bypass NoSuchElementException if empty
     */
    @SuppressWarnings("unchecked")
    public static <T> Supplier<T> completeOnEmptySequence() {
        return (Supplier<T>)COMPLETE_ON_EMPTY_SEQUENCE;
    }

    final Supplier<? extends T> defaultSupplier;

    public PublisherSingle(Publisher<? extends T> source) {
        super(source);
        this.defaultSupplier = null;
    }

    public PublisherSingle(Publisher<? extends T> source, Supplier<? extends T> defaultSupplier) {
        super(source);
        this.defaultSupplier = Objects.requireNonNull(defaultSupplier, "defaultSupplier");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherSingleSubscriber<>(s, defaultSupplier));
    }

    static final class PublisherSingleSubscriber<T> extends DeferredScalarSubscriber<T, T>
            implements Receiver {

        final Supplier<? extends T> defaultSupplier;

        Subscription s;

        int count;

        boolean done;

        public PublisherSingleSubscriber(Subscriber<? super T> actual, Supplier<? extends T> defaultSupplier) {
            super(actual);
            this.defaultSupplier = defaultSupplier;
        }

        @Override
        public void request(long n) {
            super.request(n);
            if (n > 0L) {
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }

        @Override
        public void setValue(T value) {
            this.value = value;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                subscriber.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            value = t;

            if (++count > 1) {
                cancel();

                onError(new IndexOutOfBoundsException("Source emitted more than one item"));
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;

            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

            int c = count;
            if (c == 0) {
                Supplier<? extends T> ds = defaultSupplier;
                if (ds != null) {

                    if (ds == COMPLETE_ON_EMPTY_SEQUENCE){
                        subscriber.onComplete();
                        return;
                    }

                    T t;

                    try {
                        t = ds.get();
                    } catch (Throwable e) {
                        ExceptionHelper.throwIfFatal(e);
                        subscriber.onError(ExceptionHelper.unwrap(e));
                        return;
                    }

                    if (t == null) {
                        subscriber.onError(new NullPointerException("The defaultSupplier returned a null value"));
                        return;
                    }

                    complete(t);
                } else {
                    subscriber.onError(new NoSuchElementException("Source was empty"));
                }
            } else if (c == 1) {
                complete(value);
            }
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Object connectedInput() {
            return defaultSupplier != COMPLETE_ON_EMPTY_SEQUENCE ? defaultSupplier : null;
        }

    }
}
