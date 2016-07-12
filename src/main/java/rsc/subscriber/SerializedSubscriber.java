package rsc.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rsc.flow.Producer;
import rsc.flow.Receiver;

/**
 * Subscriber that makes sure signals are delivered sequentially in case the onNext, onError or onComplete methods are
 * called concurrently.
 * <p>
 * <p>
 * The implementation uses {@code synchronized (this)} to ensure mutual exclusion.
 * <p>
 * <p>
 * Note that the class implements Subscription to save on allocation.
 *
 * @param <T> the value type
 */
public final class SerializedSubscriber<T> implements Subscriber<T>, Subscription, Receiver, Producer,
                                                      SubscriberState {

    final Subscriber<? super T> actual;

    boolean emitting;

    boolean missed;

    volatile boolean done;

    volatile boolean cancelled;

    LinkedArrayNode<T> head;

    LinkedArrayNode<T> tail;

    Throwable error;

    Subscription s;

	/**
     * Safely gate a subscriber subject concurrent Reactive Stream signals, thus serializing as a single sequence.
     * Note that serialization uses Thread Stealing and is vulnerable to cpu starving issues.
     * @param actual the subscriber to gate
     * @param <T>
     * @return a safe subscriber
     */
    public static <T> Subscriber<T> create(Subscriber<T> actual){
        return new SerializedSubscriber<>(actual);
    }

    public SerializedSubscriber(Subscriber<? super T> actual) {
        this.actual = actual;
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
        if (isCancelled() || isTerminated()) {
            return;
        }

        synchronized (this) {
            if (isCancelled() || isTerminated()) {
                return;
            }

            if (emitting) {
                serAdd(t);
                missed = true;
                return;
            }

            emitting = true;
        }

        actual.onNext(t);

        serDrainLoop(actual);
    }

    @Override
    public void onError(Throwable t) {
        if (isCancelled() || isTerminated()) {
            return;
        }

        synchronized (this) {
            if (isCancelled() || isTerminated()) {
                return;
            }

            done = true;
            error = t;

            if (emitting) {
                missed = true;
                return;
            }
        }

        actual.onError(t);
    }

    @Override
    public void onComplete() {
        if (isCancelled() || isTerminated()) {
            return;
        }

        synchronized (this) {
            if (isCancelled() || isTerminated()) {
                return;
            }

            done = true;

            if (emitting) {
                missed = true;
                return;
            }
        }

        actual.onComplete();
    }

    @Override
    public void request(long n) {
        s.request(n);
    }

    @Override
    public void cancel() {
        cancelled = true;
        s.cancel();
    }

    void serAdd(T value) {
        LinkedArrayNode<T> t = tail;

        if (t == null) {
            t = new LinkedArrayNode<>(value);

            head = t;
            tail = t;
        } else {
            if (t.count == LinkedArrayNode.DEFAULT_CAPACITY) {
                LinkedArrayNode<T> n = new LinkedArrayNode<>(value);

                t.next = n;
                tail = n ;
            } else {
                t.array[t.count++] = value;
            }
        }
    }

    void serDrainLoop(Subscriber<? super T> actual) {
        for (; ; ) {

            if (isCancelled()) {
                return;
            }

            boolean d;
            Throwable e;
            LinkedArrayNode<T> n;

            synchronized (this) {
                if (isCancelled()) {
                    return;
                }

                if (!missed) {
                    emitting = false;
                    return;
                }

                missed = false;

                d = isTerminated();
                e = getError();
                n = head;

                head = null;
                tail = null;
            }

            while (n != null) {

                T[] arr = n.array;
                int c = n.count;

                for (int i = 0; i < c; i++) {

                    if (isCancelled()) {
                        return;
                    }

                    actual.onNext(arr[i]);
                }

                n = n.next;
            }

            if (isCancelled()) {
                return;
            }

            if (e != null) {
                actual.onError(e);
                return;
            } else if (d) {
                actual.onComplete();
                return;
            }
        }
    }

    @Override
    public Subscriber<? super T> downstream() {
        return actual;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isTerminated() {
        return done;
    }

    @Override
    public Throwable getError() {
        return error;
    }

    @Override
    public boolean isStarted() {
        return s != null || !cancelled;
    }

    @Override
    public Subscription upstream() {
        return s;
    }

    @Override
    public long getPending() {
        LinkedArrayNode<T> node = tail;
        if(node != null){
            return node.count;
        }
        return 0;
    }

    @Override
    public long getCapacity() {
        return LinkedArrayNode.DEFAULT_CAPACITY;
    }

    /**
     * Node in a linked array list that is only appended.
     *
     * @param <T> the value type
     */
    static final class LinkedArrayNode<T> {

        static final int DEFAULT_CAPACITY = 16;

        final T[] array;
        int count;

        LinkedArrayNode<T> next;

        @SuppressWarnings("unchecked")
        public LinkedArrayNode(T value) {
            array = (T[]) new Object[DEFAULT_CAPACITY];
            array[0] = value;
            count = 1;
        }
    }
}
