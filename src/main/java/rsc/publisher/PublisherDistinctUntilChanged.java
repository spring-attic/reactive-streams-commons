package rsc.publisher;

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.flow.Trackable;
import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;
import rsc.flow.Fuseable.*;

/**
 * Filters out subsequent and repeated elements.
 *
 * @param <T> the value type
 * @param <K> the key type used for comparing subsequent elements
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.CONDITIONAL }, output = { FusionMode.CONDITIONAL })
public final class PublisherDistinctUntilChanged<T, K> extends PublisherSource<T, T> {

    final Function<? super T, K> keyExtractor;

    public PublisherDistinctUntilChanged(Publisher<? extends T> source, Function<? super T, K> keyExtractor) {
        super(source);
        this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new PublisherDistinctUntilChangedConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, keyExtractor));
        } else {
            source.subscribe(new PublisherDistinctUntilChangedSubscriber<>(s, keyExtractor));
        }
    }

    static final class PublisherDistinctUntilChangedSubscriber<T, K>
            implements ConditionalSubscriber<T>, Receiver, Producer, Loopback,
                       Subscription, Trackable {
        final Subscriber<? super T> actual;

        final Function<? super T, K> keyExtractor;

        Subscription s;

        boolean done;

        K lastKey;

        public PublisherDistinctUntilChangedSubscriber(Subscriber<? super T> actual,
                                                       Function<? super T, K> keyExtractor) {
            this.actual = actual;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (!tryOnNext(t)) {
                s.request(1);
            }
        }
        
        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return true;
            }

            K k;

            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return true;
            }


            if (Objects.equals(lastKey, k)) {
                lastKey = k;
                return false;
            }
            lastKey = k;
            actual.onNext(t);
            return true;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

            actual.onComplete();
        }

        @Override
        public boolean isStarted() {
            return s != null && !done;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedInput() {
            return keyExtractor;
        }

        @Override
        public Object connectedOutput() {
            return lastKey;
        }

        @Override
        public Object upstream() {
            return s;
        }
        
        @Override
        public void request(long n) {
            s.request(n);
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }
    }

    static final class PublisherDistinctUntilChangedConditionalSubscriber<T, K>
    implements ConditionalSubscriber<T>, Receiver, Producer, Loopback, Subscription,
               Trackable {
        final ConditionalSubscriber<? super T> actual;

        final Function<? super T, K> keyExtractor;

        Subscription s;

        boolean done;

        K lastKey;

        public PublisherDistinctUntilChangedConditionalSubscriber(ConditionalSubscriber<? super T> actual,
                Function<? super T, K> keyExtractor) {
            this.actual = actual;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }

            K k;

            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return;
            }


            lastKey = k;
            if (Objects.equals(lastKey, k)) {
                s.request(1);
            } else {
                actual.onNext(t);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return true;
            }

            K k;

            try {
                k = keyExtractor.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return true;
            }


            if (Objects.equals(lastKey, k)) {
                lastKey = k;
                return false;
            }
            lastKey = k;
            return actual.tryOnNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;

            actual.onComplete();
        }

        @Override
        public boolean isStarted() {
            return s != null && !done;
        }

        @Override
        public boolean isTerminated() {
            return done;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedInput() {
            return keyExtractor;
        }

        @Override
        public Object connectedOutput() {
            return lastKey;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public void request(long n) {
            s.request(n);
        }

        @Override
        public void cancel() {
            s.cancel();
        }
    }

}
