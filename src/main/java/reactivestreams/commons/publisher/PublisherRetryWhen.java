package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.error.ExceptionHelper;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.subscriber.SerializedSubscriber;
import reactivestreams.commons.subscriber.SubscriberMultiSubscription;
import reactivestreams.commons.subscription.DeferredSubscription;
import reactivestreams.commons.subscription.EmptySubscription;

/**
 * retries a source when a companion sequence signals
 * an item in response to the main's error signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 */
public final class PublisherRetryWhen<T> extends PublisherSource<T, T> {

    final Function<? super PublisherBase<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory;

    public PublisherRetryWhen(Publisher<? extends T> source,
                              Function<? super PublisherBase<Throwable>, ? extends Publisher<? extends Object>> whenSourceFactory) {
        super(source);
        this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        PublisherRetryWhenOtherSubscriber other = new PublisherRetryWhenOtherSubscriber();
        other.completionSignal.onSubscribe(EmptySubscription.INSTANCE);

        SerializedSubscriber<T> serial = new SerializedSubscriber<>(s);

        PublisherRetryWhenMainSubscriber<T> main = new PublisherRetryWhenMainSubscriber<>(serial, other
          .completionSignal, source);
        other.main = main;

        serial.onSubscribe(main);

        Publisher<? extends Object> p;

        try {
            p = whenSourceFactory.apply(other);
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            s.onError(ExceptionHelper.unwrap(e));
            return;
        }

        if (p == null) {
            s.onError(new NullPointerException("The whenSourceFactory returned a null Publisher"));
            return;
        }

        p.subscribe(other);

        if (!main.cancelled) {
            source.subscribe(main);
        }
    }

    static final class PublisherRetryWhenMainSubscriber<T> extends SubscriberMultiSubscription<T, T> {

        final DeferredSubscription otherArbiter;

        final Subscriber<Throwable> signaller;

        final Publisher<? extends T> source;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRetryWhenMainSubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherRetryWhenMainSubscriber.class, "wip");

        volatile boolean cancelled;

        public PublisherRetryWhenMainSubscriber(Subscriber<? super T> actual, Subscriber<Throwable> signaller,
                                                Publisher<? extends T> source) {
            super(actual);
            this.signaller = signaller;
            this.source = source;
            this.otherArbiter = new DeferredSubscription();
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;

            cancelWhen();

            super.cancel();
        }

        void cancelWhen() {
            otherArbiter.cancel();
        }

        public void setWhen(Subscription w) {
            otherArbiter.set(w);
        }

        @Override
        public void onNext(T t) {
            subscriber.onNext(t);

            producedOne();
        }

        @Override
        public void onError(Throwable t) {
            otherArbiter.request(1);

            signaller.onNext(t);
        }

        @Override
        public void onComplete() {
            otherArbiter.cancel();

            subscriber.onComplete();
        }

        void resubscribe() {
            if (WIP.getAndIncrement(this) == 0) {
                do {
                    if (cancelled) {
                        return;
                    }

                    source.subscribe(this);

                } while (WIP.decrementAndGet(this) != 0);
            }
        }

        void whenError(Throwable e) {
            cancelled = true;
            super.cancel();

            subscriber.onError(e);
        }

        void whenComplete() {
            cancelled = true;
            super.cancel();

            subscriber.onComplete();
        }
    }

    static final class PublisherRetryWhenOtherSubscriber
    extends PublisherBase<Throwable>
    implements Subscriber<Object>, FeedbackLoop, Trace, Inner {
        PublisherRetryWhenMainSubscriber<?> main;

        final SimpleProcessor<Throwable> completionSignal = new SimpleProcessor<>();

        @Override
        public void onSubscribe(Subscription s) {
            main.setWhen(s);
        }

        @Override
        public void onNext(Object t) {
            main.resubscribe();
        }

        @Override
        public void onError(Throwable t) {
            main.whenError(t);
        }

        @Override
        public void onComplete() {
            main.whenComplete();
        }

        @Override
        public void subscribe(Subscriber<? super Throwable> s) {
            completionSignal.subscribe(s);
        }

        @Override
        public Object delegateInput() {
            return main;
        }

        @Override
        public Object delegateOutput() {
            return completionSignal;
        }
    }
}
