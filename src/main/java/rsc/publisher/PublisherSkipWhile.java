package rsc.publisher;

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.flow.*;
import rsc.state.Completable;
import rsc.util.ExceptionHelper;
import rsc.util.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;
import rsc.flow.Fuseable.*;

/**
 * Skips source values while a predicate returns
 * true for the value.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.CONDITIONAL }, output = { FusionMode.CONDITIONAL }) // TODO SYNC/ASYNC???
public final class PublisherSkipWhile<T> extends PublisherSource<T, T> {

    final Predicate<? super T> predicate;

    public PublisherSkipWhile(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public Publisher<? extends T> source() {
        return source;
    }

    public Predicate<? super T> predicate() {
        return predicate;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherSkipWhileSubscriber<>(s, predicate));
    }

    static final class PublisherSkipWhileSubscriber<T> implements ConditionalSubscriber<T>, Receiver, Producer, Loopback,
                                                                  Completable, Subscription {
        final Subscriber<? super T> actual;

        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        boolean skipped;

        public PublisherSkipWhileSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
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

            if (skipped){
                actual.onNext(t);
                return;
            }
            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));

                return;
            }

            if (b) {
                s.request(1);

                return;
            }

            skipped = true;
            actual.onNext(t);
        }
        
        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return true;
            }

            if (skipped){
                actual.onNext(t);
                return true;
            }
            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));

                return true;
            }

            if (b) {
                return false;
            }

            skipped = true;
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
            return predicate;
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
    
    static final class PublisherSkipWhileConditionalSubscriber<T> implements ConditionalSubscriber<T>, Receiver, Producer, Loopback,
    Completable, Subscription {
        final ConditionalSubscriber<? super T> actual;

        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        boolean skipped;

        public PublisherSkipWhileConditionalSubscriber(ConditionalSubscriber<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
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

            if (skipped){
                actual.onNext(t);
                return;
            }
            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));

                return;
            }

            if (b) {
                s.request(1);

                return;
            }

            skipped = true;
            actual.onNext(t);
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return true;
            }

            if (skipped){
                return actual.tryOnNext(t);
            }
            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));

                return true;
            }

            if (b) {
                return false;
            }

            skipped = true;
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
            return predicate;
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
