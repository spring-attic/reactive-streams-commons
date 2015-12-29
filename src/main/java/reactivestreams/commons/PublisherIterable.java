package reactivestreams.commons;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.BackpressureHelper;
import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Emits the contents of an Iterable source.
 *
 * @param <T> the value type
 */
public final class PublisherIterable<T> implements Publisher<T> {

    final Iterable<? extends T> iterable;

    public PublisherIterable(Iterable<? extends T> iterable) {
        this.iterable = Objects.requireNonNull(iterable, "iterable");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Iterator<? extends T> it;

        try {
            it = iterable.iterator();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        subscribe(s, it);
    }

    /**
     * Common method to take an Iterator as a source of values.
     * @param s
     * @param it
     */
    static <T> void subscribe(Subscriber<? super T> s, Iterator<? extends T> it) {
        if (it == null) {
            EmptySubscription.error(s, new NullPointerException("The iterator is null"));
            return;
        }

        boolean b;

        try {
            b = it.hasNext();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        if (!b) {
            EmptySubscription.complete(s);
            return;
        }

        s.onSubscribe(new PublisherIterableSubscription<>(s, it));
    }

    static final class PublisherIterableSubscription<T>
    implements Subscription {

        final Subscriber<? super T> actual;

        final Iterator<? extends T> iterator;

        volatile boolean cancelled;

        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherIterableSubscription> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherIterableSubscription.class, "requested");

        public PublisherIterableSubscription(Subscriber<? super T> actual, Iterator<? extends T> iterator) {
            this.actual = actual;
            this.iterator = iterator;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.addAndGet(REQUESTED, this, n) == 0) {
                    if (n == Long.MAX_VALUE) {
                        fastPath();
                    } else {
                        slowPath(n);
                    }
                }
            }
        }

        void slowPath(long n) {
            final Iterator<? extends T> a = iterator;
            final Subscriber<? super T> s = actual;

            long e = 0L;

            for (;;) {

                while (e != n) {
                    T t;

                    try {
                        t = a.next();
                    } catch (Throwable ex) {
                        s.onError(ex);
                        return;
                    }

                    if (cancelled) {
                        return;
                    }

                    if (t == null) {
                        s.onError(new NullPointerException("The iterator returned a null value"));
                        return;
                    }

                    s.onNext(t);

                    if (cancelled) {
                        return;
                    }

                    boolean b;

                    try {
                        b = a.hasNext();
                    } catch (Throwable ex) {
                        s.onError(ex);
                        return;
                    }

                    if (cancelled) {
                        return;
                    }

                    if (!b) {
                        s.onComplete();
                        return;
                    }

                    e++;
                }

                n = requested;

                if (n == e) {
                    n = REQUESTED.addAndGet(this, -e);
                    if (n == 0L) {
                        return;
                    }
                    e = 0L;
                }
            }
        }

        void fastPath() {
            final Iterator<? extends T> a = iterator;
            final Subscriber<? super T> s = actual;

            for (;;) {

                if (cancelled) {
                    return;
                }

                T t;

                try {
                    t = a.next();
                } catch (Exception ex) {
                    s.onError(ex);
                    return;
                }

                if (cancelled) {
                    return;
                }

                if (t == null) {
                    s.onError(new NullPointerException("The iterator returned a null value"));
                    return;
                }

                s.onNext(t);

                if (cancelled) {
                    return;
                }

                boolean b;

                try {
                    b = a.hasNext();
                } catch (Exception ex) {
                    s.onError(ex);
                    return;
                }

                if (cancelled) {
                    return;
                }

                if (!b) {
                    s.onComplete();
                    return;
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
