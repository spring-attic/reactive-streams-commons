package rsc.publisher;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.documentation.Operator;
import rsc.documentation.OperatorType;
import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;
import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Emits a single boolean true if all values of the source sequence match
 * the predicate.
 * <p>
 * The implementation uses short-circuit logic and completes with false if
 * the predicate doesn't match a value.
 *
 * @param <T> the source value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
@Operator(traits = {OperatorType.CONDITIONAL, OperatorType.TRANSFORMATION}, aliases =
        "all")
public final class PublisherAll<T> extends PublisherSource<T, Boolean> implements Fuseable {

    final Predicate<? super T> predicate;

    public PublisherAll(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherAllSubscriber<T>(s, predicate));
    }

    static final class PublisherAllSubscriber<T> extends DeferredScalarSubscriber<T, Boolean>
            implements Receiver {
        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        public PublisherAllSubscriber(Subscriber<? super Boolean> actual, Predicate<? super T> predicate) {
            super(actual);
            this.predicate = predicate;
        }

        @Override
        public void cancel() {
            s.cancel();
            super.cancel();
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
                return;
            }

            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                done = true;
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                subscriber.onError(ExceptionHelper.unwrap(e));
                return;
            }
            if (!b) {
                done = true;
                s.cancel();

                complete(false);
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
            complete(true);
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Object connectedInput() {
            return predicate;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }
    }
}
