package rsc.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.flow.Loopback;
import rsc.processor.DirectProcessor;
import rsc.subscriber.MultiSubscriptionSubscriber;
import rsc.subscriber.SerializedSubscriber;
import rsc.util.DeferredSubscription;
import rsc.util.EmptySubscription;
import rsc.util.ExceptionHelper;

/**
 * Repeats a source when a companion sequence
 * signals an item in response to the main's completion signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 */
public final class PublisherRepeatWhen<T> extends PublisherSource<T, T> {

    final Function<? super Px<Long>, ? extends Publisher<? extends Object>> whenSourceFactory;

    public PublisherRepeatWhen(Publisher<? extends T> source,
                               Function<? super Px<Long>, ? extends Publisher<? extends Object>> whenSourceFactory) {
        super(source);
        this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
    }

    @Override
    public long getCapacity() {
        return -1L;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        PublisherRepeatWhenOtherSubscriber other = new PublisherRepeatWhenOtherSubscriber();
        Subscriber<Long> signaller = new SerializedSubscriber<>(other.completionSignal);
        
        signaller.onSubscribe(EmptySubscription.INSTANCE);

        Subscriber<T> serial = new SerializedSubscriber<>(s);

        PublisherRepeatWhenMainSubscriber<T> main = new PublisherRepeatWhenMainSubscriber<>(serial, signaller, source);
        other.main = main;

        serial.onSubscribe(main);

        Publisher<?> p;

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

    static final class PublisherRepeatWhenMainSubscriber<T> extends MultiSubscriptionSubscriber<T, T> {

        final DeferredSubscription otherArbiter;

        final Subscriber<Long> signaller;

        final Publisher<? extends T> source;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRepeatWhenMainSubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherRepeatWhenMainSubscriber.class, "wip");

        volatile boolean cancelled;

        long produced;

        public PublisherRepeatWhenMainSubscriber(Subscriber<? super T> actual, Subscriber<Long> signaller,
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

        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }

        @Override
        public void onNext(T t) {
            subscriber.onNext(t);

            produced++;
        }

        @Override
        public void onError(Throwable t) {
            otherArbiter.cancel();

            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            long p = produced;
            if (p != 0L) {
                produced = 0;
                produced(p);
            }

            otherArbiter.request(1);
            signaller.onNext(p);
        }

        void cancelWhen() {
            otherArbiter.cancel();
        }

        void setWhen(Subscription w) {
            otherArbiter.set(w);
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

    static final class PublisherRepeatWhenOtherSubscriber 
    extends Px<Long>
    implements Subscriber<Object>, Loopback {
        PublisherRepeatWhenMainSubscriber<?> main;

        final DirectProcessor<Long> completionSignal = new DirectProcessor<>();

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
        public void subscribe(Subscriber<? super Long> s) {
            completionSignal.subscribe(s);
        }

        @Override
        public Object connectedInput() {
            return main;
        }

        @Override
        public Object connectedOutput() {
            return completionSignal;
        }

        @Override
        public int getMode() {
            return INNER | TRACE_ONLY;
        }
    }
}
