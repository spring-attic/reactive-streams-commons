package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactivestreams.commons.flow.MultiReceiver;
import reactivestreams.commons.subscriber.MultiSubscriptionSubscriber;
import reactivestreams.commons.util.EmptySubscription;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 */
public final class PublisherConcatArray<T> 
extends PublisherBase<T>
        implements MultiReceiver {

    final Publisher<? extends T>[] array;

    @SafeVarargs
    public PublisherConcatArray(Publisher<? extends T>... array) {
        this.array = Objects.requireNonNull(array, "array");
    }

    @Override
    public Iterator<?> upstreams() {
        return Arrays.asList(array).iterator();
    }

    @Override
    public long upstreamCount() {
        return array.length;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T>[] a = array;

        if (a.length == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (a.length == 1) {
            Publisher<? extends T> p = a[0];

            if (p == null) {
                EmptySubscription.error(s, new NullPointerException("The single source Publisher is null"));
            } else {
                p.subscribe(s);
            }
            return;
        }

        PublisherConcatArraySubscriber<T> parent = new PublisherConcatArraySubscriber<>(s, a);

        s.onSubscribe(parent);

        if (!parent.isCancelled()) {
            parent.onComplete();
        }
    }

    static final class PublisherConcatArraySubscriber<T>
            extends MultiSubscriptionSubscriber<T, T> {

        final Publisher<? extends T>[] sources;

        int index;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatArraySubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherConcatArraySubscriber.class, "wip");

        long produced;

        public PublisherConcatArraySubscriber(Subscriber<? super T> actual, Publisher<? extends T>[] sources) {
            super(actual);
            this.sources = sources;
        }

        @Override
        public void onNext(T t) {
            produced++;

            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            if (WIP.getAndIncrement(this) == 0) {
                Publisher<? extends T>[] a = sources;
                do {

                    if (isCancelled()) {
                        return;
                    }

                    int i = index;
                    if (i == a.length) {
                        subscriber.onComplete();
                        return;
                    }

                    Publisher<? extends T> p = a[i];

                    if (p == null) {
                        subscriber.onError(new NullPointerException("The " + i + "th source Publisher is null"));
                        return;
                    }

                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        produced(c);
                    }
                    p.subscribe(this);

                    if (isCancelled()) {
                        return;
                    }

                    index = ++i;
                } while (WIP.decrementAndGet(this) != 0);
            }

        }
    }
}
