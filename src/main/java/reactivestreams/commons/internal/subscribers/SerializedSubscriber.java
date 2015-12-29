package reactivestreams.commons.internal.subscribers;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Subscriber that makes sure signals are delivered sequentially in case
 * the onNext, onError or onComplete methods are called concurrently.
 *
 * <p>
 * The implementation uses {@code synchronized (this)} to ensure mutual exclusion.
 * 
 * <p>
 * Note that the class implements Subscription to save on allocation.
 * 
 * @param <T> the value type
 */
public final class SerializedSubscriber<T> implements Subscriber<T>, Subscription {

    final Subscriber<? super T> actual;
    
    boolean emitting;
    
    boolean missed;
    
    volatile boolean done;
    
    volatile boolean cancelled;
    
    LinkedArrayNode<T> head;
    
    LinkedArrayNode<T> tail;
    
    Throwable error;

    Subscription s;
    
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
        serOnNext(t);
    }
    
    @Override
    public void onError(Throwable t) {
        serOnError(t);
    }

    @Override
    public void onComplete() {
        serOnComplete();
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

    public void serAdd(T value) {
        LinkedArrayNode<T> t = serGetTail();

        if (t == null) {
            t = new LinkedArrayNode<>(value);

            serSetHead(t);
            serSetTail(t);
        } else {
            if (t.count == LinkedArrayNode.DEFAULT_CAPACITY) {
                LinkedArrayNode<T> n = new LinkedArrayNode<>(value);

                t.next = n;
                serSetTail(n);
            } else {
                t.array[t.count++] = value;
            }
        }
    }

    public void serDrainLoop(Subscriber<? super T> actual) {
        for (;;) {

            if (serIsCancelled()) {
                return;
            }

            boolean d;
            Throwable e;
            LinkedArrayNode<T> n;

            synchronized (serGuard()) {
                if (serIsCancelled()) {
                    return;
                }

                if (!serIsMissed()) {
                    serSetEmitting(false);
                    return;
                }

                serSetMissed(false);

                d = serIsDone();
                e = serGetError();
                n = serGetHead();

                serSetHead(null);
                serSetTail(null);
            }

            while (n != null) {

                T[] arr = n.array;
                int c = n.count;

                for (int i = 0; i < c; i++) {

                    if (serIsCancelled()) {
                        return;
                    }

                    actual.onNext(arr[i]);
                }

                n = n.next;
            }

            if (serIsCancelled()) {
                return;
            }

            if (e != null) {
                actual.onError(e);
                return;
            } else
            if (d) {
                actual.onComplete();
                return;
            }
        }
    }

    public Subscriber<? super T> serGetSubscriber() {
        return actual;
    }

    public Object serGuard() {
        return this;
    }

    public boolean serIsEmitting() {
        return emitting;
    }

    public void serOnComplete() {
        if (serIsCancelled() || serIsDone()) {
            return;
        }

        synchronized (this) {
            if (serIsCancelled() || serIsDone()) {
                return;
            }

            serSetDone(true);

            if (serIsEmitting()) {
                serSetMissed(true);
                return;
            }
        }

        serGetSubscriber().onComplete();
    }

    public void serOnError(Throwable e) {
        if (serIsCancelled() || serIsDone()) {
            return;
        }

        synchronized (serGuard()) {
            if (serIsCancelled() || serIsDone()) {
                return;
            }

            serSetDone(true);
            serSetError(e);

            if (serIsEmitting()) {
                serSetMissed(true);
                return;
            }
        }

        serGetSubscriber().onError(e);
    }

    public void serOnNext(T t) {
        if (serIsCancelled() || serIsDone()) {
            return;
        }

        synchronized (serGuard()) {
            if (serIsCancelled() || serIsDone()) {
                return;
            }

            if (serIsEmitting()) {
                serAdd(t);
                serSetMissed(true);
                return;
            }

            serSetEmitting(true);
        }

        Subscriber<? super T> actual = serGetSubscriber();

        actual.onNext(t);

        serDrainLoop(actual);
    }

    public void serSetEmitting(boolean emitting) {
        this.emitting = emitting;
    }

    public boolean serIsMissed() {
        return missed;
    }

    public void serSetMissed(boolean missed) {
        this.missed = missed;
    }

    public boolean serIsCancelled() {
        return cancelled;
    }

    public boolean serIsDone() {
        return done;
    }

    public void serSetDone(boolean done) {
        this.done = done;
    }

    public Throwable serGetError() {
        return error;
    }

    public void serSetError(Throwable error) {
        this.error = error;
    }

    public LinkedArrayNode<T> serGetHead() {
        return head;
    }

    public void serSetHead(LinkedArrayNode<T> node) {
        head = node;
    }

    public LinkedArrayNode<T> serGetTail() {
        return tail;
    }

    public void serSetTail(LinkedArrayNode<T> node) {
        tail = node;
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
            array = (T[])new Object[DEFAULT_CAPACITY];
            array[0] = value;
            count = 1;
        }
    }
}
