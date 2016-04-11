package reactivestreams.commons.publisher;

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
public final class PublisherConcatIterable<T> 
extends Px<T>
        implements MultiReceiver {

    final Iterable<? extends Publisher<? extends T>> iterable;

    public PublisherConcatIterable(Iterable<? extends Publisher<? extends T>> iterable) {
        this.iterable = Objects.requireNonNull(iterable, "iterable");
    }

    @Override
    public Iterator<?> upstreams() {
        return iterable.iterator();
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {

        Iterator<? extends Publisher<? extends T>> it;

        try {
            it = iterable.iterator();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        if (it == null) {
            EmptySubscription.error(s, new NullPointerException("The Iterator returned is null"));
            return;
        }

        PublisherConcatIterableSubscriber<T> parent = new PublisherConcatIterableSubscriber<>(s, it);

        s.onSubscribe(parent);

        if (!parent.isCancelled()) {
            parent.onComplete();
        }
    }

    static final class PublisherConcatIterableSubscriber<T>
            extends MultiSubscriptionSubscriber<T, T> {

        final Iterator<? extends Publisher<? extends T>> it;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatIterableSubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherConcatIterableSubscriber.class, "wip");

        long produced;

        public PublisherConcatIterableSubscriber(Subscriber<? super T> actual, Iterator<? extends Publisher<? extends
          T>> it) {
            super(actual);
            this.it = it;
        }

        @Override
        public void onNext(T t) {
            produced++;

            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            if (WIP.getAndIncrement(this) == 0) {
                Iterator<? extends Publisher<? extends T>> a = this.it;
                do {
                    if (isCancelled()) {
                        return;
                    }

                    boolean b;

                    try {
                        b = a.hasNext();
                    } catch (Throwable e) {
                        onError(e);
                        return;
                    }

                    if (isCancelled()) {
                        return;
                    }


                    if (!b) {
                        subscriber.onComplete();
                        return;
                    }

                    Publisher<? extends T> p;

                    try {
                        p = it.next();
                    } catch (Throwable e) {
                        subscriber.onError(e);
                        return;
                    }

                    if (isCancelled()) {
                        return;
                    }

                    if (p == null) {
                        subscriber.onError(new NullPointerException("The Publisher returned by the iterator is null"));
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

                } while (WIP.decrementAndGet(this) != 0);
            }

        }
    }
}
