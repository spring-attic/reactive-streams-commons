package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.subscriber.SubscriberDeferSubscription;
import reactivestreams.commons.subscription.EmptySubscription;
import reactivestreams.commons.support.ReactiveState;
import reactivestreams.commons.support.SubscriptionHelper;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Given a set of source Publishers the values of that Publisher is forwarded to the
 * subscriber which responds first with any signal.
 *
 * @param <T> the value type
 */
public final class PublisherAmb<T> implements Publisher<T>,
                                              ReactiveState.Factory,
                                              ReactiveState.LinkedUpstreams{

    final Publisher<? extends T>[] array;

    final Iterable<? extends Publisher<? extends T>> iterable;

    @SafeVarargs
    public PublisherAmb(Publisher<? extends T>... array) {
        this.array = Objects.requireNonNull(array, "array");
        this.iterable = null;
    }

    public PublisherAmb(Iterable<? extends Publisher<? extends T>> iterable) {
        this.array = null;
        this.iterable = Objects.requireNonNull(iterable);
    }

    @Override
    public Iterator<?> upstreams() {
        return iterable != null ? iterable.iterator() : Arrays.asList(array).iterator();
    }

    @Override
    public long upstreamsCount() {
        return array != null ? array.length : -1L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T>[] a = array;
        int n;
        if (a == null) {
            n = 0;
            a = new Publisher[8];

            Iterator<? extends Publisher<? extends T>> it;

            try {
                it = iterable.iterator();
            } catch (Throwable e) {
                EmptySubscription.error(s, e);
                return;
            }

            if (it == null) {
                EmptySubscription.error(s, new NullPointerException("The iterator returned is null"));
                return;
            }


            for (; ; ) {

                boolean b;

                try {
                    b = it.hasNext();
                } catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }

                if (!b) {
                    break;
                }

                Publisher<? extends T> p;

                try {
                    p = it.next();
                } catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }

                if (p == null) {
                    EmptySubscription.error(s, new NullPointerException("The Publisher returned by the iterator is " +
                      "null"));
                    return;
                }

                if (n == a.length) {
                    Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
                    System.arraycopy(a, 0, c, 0, n);
                    a = c;
                }
                a[n++] = p;
            }

        } else {
            n = a.length;
        }

        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (n == 1) {
            Publisher<? extends T> p = a[0];

            if (p == null) {
                EmptySubscription.error(s, new NullPointerException("The single source Publisher is null"));
            } else {
                p.subscribe(s);
            }
            return;
        }

        PublisherAmbCoordinator<T> coordinator = new PublisherAmbCoordinator<>(n);

        coordinator.subscribe(a, n, s);
    }

    static final class PublisherAmbCoordinator<T>
      implements Subscription, LinkedUpstreams, ActiveDownstream {

        final PublisherAmbSubscriber<T>[] subscribers;

        volatile boolean cancelled;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherAmbCoordinator> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherAmbCoordinator.class, "wip");

        @SuppressWarnings("unchecked")
        public PublisherAmbCoordinator(int n) {
            subscribers = new PublisherAmbSubscriber[n];
            wip = Integer.MIN_VALUE;
        }

        void subscribe(Publisher<? extends T>[] sources, int n, Subscriber<? super T> actual) {
            PublisherAmbSubscriber<T>[] a = subscribers;

            for (int i = 0; i < n; i++) {
                a[i] = new PublisherAmbSubscriber<>(actual, this, i);
            }

            actual.onSubscribe(this);

            for (int i = 0; i < n; i++) {
                if (cancelled || wip != Integer.MIN_VALUE) {
                    return;
                }

                Publisher<? extends T> p = sources[i];

                if (p == null) {
                    if (WIP.compareAndSet(this, Integer.MIN_VALUE, -1)) {
                        actual.onError(new NullPointerException("The " + i + " th Publisher source is null"));
                    }
                    return;
                }

                p.subscribe(a[i]);
            }

        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                int w = wip;
                if (w >= 0) {
                    subscribers[w].request(n);
                } else {
                    for (PublisherAmbSubscriber<T> s : subscribers) {
                        s.request(n);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;

            int w = wip;
            if (w >= 0) {
                subscribers[w].cancel();
            } else {
                for (PublisherAmbSubscriber<T> s : subscribers) {
                    s.cancel();
                }
            }
        }

        boolean tryWin(int index) {
            if (wip == Integer.MIN_VALUE) {
                if (WIP.compareAndSet(this, Integer.MIN_VALUE, index)) {

                    PublisherAmbSubscriber<T>[] a = subscribers;
                    int n = a.length;

                    for (int i = 0; i < n; i++) {
                        if (i != index) {
                            a[i].cancel();
                        }
                    }

                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public Iterator<?> upstreams() {
            return Arrays.asList(subscribers).iterator();
        }

        @Override
        public long upstreamsCount() {
            return subscribers.length;
        }
    }

    static final class PublisherAmbSubscriber<T> extends SubscriberDeferSubscription<T, T>
    implements Inner {
        final PublisherAmbCoordinator<T> parent;

        final int index;

        boolean won;

        public PublisherAmbSubscriber(Subscriber<? super T> actual, PublisherAmbCoordinator<T> parent, int index) {
            super(actual);
            this.parent = parent;
            this.index = index;
        }

        @Override
        public void onNext(T t) {
            if (won) {
                subscriber.onNext(t);
            } else if (parent.tryWin(index)) {
                won = true;
                subscriber.onNext(t);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (won) {
                subscriber.onError(t);
            } else if (parent.tryWin(index)) {
                won = true;
                subscriber.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (won) {
                subscriber.onComplete();
            } else if (parent.tryWin(index)) {
                won = true;
                subscriber.onComplete();
            }
        }
    }
}
