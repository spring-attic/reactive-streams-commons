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
import rsc.flow.*;
import rsc.subscriber.SubscriberState;
import rsc.util.ExceptionHelper;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.UnsignalledExceptions;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 */
@BackpressureSupport(input = BackpressureMode.BOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL }, output = { FusionMode.SYNC, FusionMode.ASYNC, FusionMode.CONDITIONAL, FusionMode.BOUNDARY })
public final class PublisherFilter<T> extends PublisherSource<T, T> {

    final Predicate<? super T> predicate;

    public PublisherFilter(Publisher<? extends T> source, Predicate<? super T> predicate) {
        super(source);
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public Predicate<? super T> predicate() {
        return predicate;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (source instanceof Fuseable) {
            if (s instanceof Fuseable.ConditionalSubscriber) {
                source.subscribe(new PublisherFilterFuseable.FilterFuseableConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>)s, predicate));
                return;
            }
            source.subscribe(new PublisherFilterFuseable.FilterFuseableSubscriber<>(s, predicate));
            return;
        }
        if (s instanceof Fuseable.ConditionalSubscriber) {
            source.subscribe(new FilterConditionalSubscriber<>((Fuseable.ConditionalSubscriber<? super T>)s, predicate));
            return;
        }
        source.subscribe(new FilterSubscriber<>(s, predicate));
    }

    static final class FilterSubscriber<T>
            implements Receiver, Producer, Loopback, Subscription, Fuseable.ConditionalSubscriber<T>, SubscriberState {
        final Subscriber<? super T> actual;

        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        public FilterSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
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

            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                s.cancel();

                ExceptionHelper.throwIfFatal(e);
                onError(ExceptionHelper.unwrap(e));
                return;
            }
            if (b) {
                actual.onNext(t);
            } else {
                s.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return false;
            }

            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                s.cancel();

                onError(ExceptionHelper.unwrap(e));
                return false;
            }
            if (b) {
                actual.onNext(t);
                return true;
            }
            return false;
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

    static final class FilterConditionalSubscriber<T> 
    implements Receiver, Producer, Loopback, Subscription, Fuseable.ConditionalSubscriber<T>,
               SubscriberState {
        final Fuseable.ConditionalSubscriber<? super T> actual;

        final Predicate<? super T> predicate;

        Subscription s;

        boolean done;

        public FilterConditionalSubscriber(Fuseable.ConditionalSubscriber<? super T> actual, Predicate<? super T> predicate) {
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

            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                s.cancel();

                onError(ExceptionHelper.unwrap(e));
                return;
            }
            if (b) {
                actual.onNext(t);
            } else {
                s.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return false;
            }

            boolean b;

            try {
                b = predicate.test(t);
            } catch (Throwable e) {
                ExceptionHelper.throwIfFatal(e);
                s.cancel();

                onError(ExceptionHelper.unwrap(e));
                return false;
            }
            if (b) {
                return actual.tryOnNext(t);
            }
            return false;
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
