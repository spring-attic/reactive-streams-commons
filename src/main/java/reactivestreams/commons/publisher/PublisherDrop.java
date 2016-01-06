package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.*;

import reactivestreams.commons.error.UnsignalledExceptions;
import reactivestreams.commons.support.*;

/**
 * Drops values if the subscriber doesn't request fast enough.
 *
 * @param <T> the value type
 */
public final class PublisherDrop<T> extends PublisherSource<T, T> {

    static final Consumer<Object> NOOP = new Consumer<Object>() {
        @Override
        public void accept(Object t) {

        }
    };

    final Consumer<? super T> onDrop;

    public PublisherDrop(Publisher<? extends T> source) {
        super(source);
        this.onDrop = NOOP;
    }


    public PublisherDrop(Publisher<? extends T> source, Consumer<? super T> onDrop) {
        super(source);
        this.onDrop = Objects.requireNonNull(onDrop, "onDrop");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherDropSubscriber<>(s, onDrop));
    }

    static final class PublisherDropSubscriber<T>
            implements Subscriber<T>, Subscription, Downstream, Upstream, ActiveUpstream,
                       DownstreamDemand, FeedbackLoop {

        final Subscriber<? super T> actual;

        final Consumer<? super T> onDrop;

        Subscription s;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherDropSubscriber> REQUESTED =
          AtomicLongFieldUpdater.newUpdater(PublisherDropSubscriber.class, "requested");

        boolean done;

        public PublisherDropSubscriber(Subscriber<? super T> actual, Consumer<? super T> onDrop) {
            this.actual = actual;
            this.onDrop = onDrop;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
            }
        }

        @Override
        public void cancel() {
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);

                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {

            if (done) {
                return;
            }

            if (requested != 0L) {

                actual.onNext(t);

                if (requested != Long.MAX_VALUE) {
                    REQUESTED.decrementAndGet(this);
                }

            } else {
                try {
                    onDrop.accept(t);
                } catch (Throwable e) {
                    cancel();

                    onError(e);
                }
            }
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
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public Object delegateInput() {
            return onDrop;
        }

        @Override
        public Object delegateOutput() {
            return null;
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
