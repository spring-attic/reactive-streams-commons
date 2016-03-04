package reactivestreams.commons.publisher;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.subscriber.MultiSubscriptionSubscriber;

/**
 * Repeatedly subscribes to the source sequence if it signals any error
 * either indefinitely or a fixed number of times.
 * <p>
 * The times == Long.MAX_VALUE is treated as infinite retry.
 *
 * @param <T> the value type
 */
public final class PublisherRetry<T> extends PublisherSource<T, T> {

    final long times;

    public PublisherRetry(Publisher<? extends T> source) {
        this(source, Long.MAX_VALUE);
    }

    public PublisherRetry(Publisher<? extends T> source, long times) {
        super(source);
        if (times < 0L) {
            throw new IllegalArgumentException("times >= 0 required");
        }
        this.times = times;
    }

    @Override
    public long getCapacity() {
        return -1L;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        PublisherRetrySubscriber<T> parent = new PublisherRetrySubscriber<>(source, s, times);

        s.onSubscribe(parent);

        if (!parent.isCancelled()) {
            parent.resubscribe();
        }
    }

    static final class PublisherRetrySubscriber<T>
            extends MultiSubscriptionSubscriber<T, T> {

        final Publisher<? extends T> source;

        long remaining;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRetrySubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherRetrySubscriber.class, "wip");

        long produced;

        public PublisherRetrySubscriber(Publisher<? extends T> source, Subscriber<? super T> actual, long remaining) {
            super(actual);
            this.source = source;
            this.remaining = remaining;
        }

        @Override
        public void onNext(T t) {
            produced++;

            subscriber.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            long r = remaining;
            if (r != Long.MAX_VALUE) {
                if (r == 0) {
                    subscriber.onError(t);
                    return;
                }
                remaining = r - 1;
            }

            resubscribe();
        }

        void resubscribe() {
            if (WIP.getAndIncrement(this) == 0) {
                do {
                    if (isCancelled()) {
                        return;
                    }

                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        produced(c);
                    }

                    source.subscribe(this);

                } while (WIP.decrementAndGet(this) != 0);
            }
        }
    }
}
