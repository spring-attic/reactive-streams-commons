package rsc.publisher;

import java.util.Objects;
import java.util.function.BiConsumer;
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
 * Collects the values of the source sequence into a container returned by
 * a supplier and a collector action working on the container and the current source
 * value.
 *
 * @param <T> the source value type
 * @param <R> the container value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherCollect<T, R> extends PublisherSource<T, R> implements Fuseable {

    final Supplier<R> supplier;

    final BiConsumer<? super R, ? super T> action;

    public PublisherCollect(Publisher<? extends T> source, Supplier<R> supplier,
                            BiConsumer<? super R, ? super T> action) {
        super(source);
        this.supplier = Objects.requireNonNull(supplier, "supplier");
        this.action = Objects.requireNonNull(action);
    }

    @Override
    public long getPrefetch() {
        return Long.MAX_VALUE;
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        R container;

        try {
            container = supplier.get();
        } catch (Throwable e) {
            SubscriptionHelper.error(s, e);
            return;
        }

        if (container == null) {
            SubscriptionHelper.error(s, new NullPointerException("The supplier returned a null container"));
            return;
        }

        source.subscribe(new PublisherCollectSubscriber<>(s, action, container));
    }

    static final class PublisherCollectSubscriber<T, R>
            extends DeferredScalarSubscriber<T, R>
            implements Receiver {

        final BiConsumer<? super R, ? super T> action;

        Subscription s;

        boolean done;

        public PublisherCollectSubscriber(Subscriber<? super R> actual, BiConsumer<? super R, ? super T> action,
                                          R container) {
            super(actual);
            this.action = action;
            this.value = container;
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                subscriber.onSubscribe(this);

                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }

            try {
                action.accept(value, t);
            } catch (Throwable e) {
                cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
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
            complete(value);
        }

        @Override
        public void setValue(R value) {
            // value is constant
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
            return action;
        }

        @Override
        public Object connectedOutput() {
            return value;
        }
    }
}
