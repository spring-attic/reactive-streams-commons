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
public final class SerializedSubscriber<T> implements Subscriber<T>, 
Subscription, SubscriberSignalSerializerTrait<T> {

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

    @Override
    public Subscriber<? super T> serGetSubscriber() {
        return actual;
    }

    @Override
    public Object serGuard() {
        return this;
    }

    @Override
    public boolean serIsEmitting() {
        return emitting;
    }

    @Override
    public void serSetEmitting(boolean emitting) {
        this.emitting = emitting;
    }

    @Override
    public boolean serIsMissed() {
        return missed;
    }

    @Override
    public void serSetMissed(boolean missed) {
        this.missed = missed;
    }

    @Override
    public boolean serIsCancelled() {
        return cancelled;
    }

    @Override
    public boolean serIsDone() {
        return done;
    }

    @Override
    public void serSetDone(boolean done) {
        this.done = done;
    }

    @Override
    public Throwable serGetError() {
        return error;
    }

    @Override
    public void serSetError(Throwable error) {
        this.error = error;
    }

    @Override
    public LinkedArrayNode<T> serGetHead() {
        return head;
    }

    @Override
    public void serSetHead(LinkedArrayNode<T> node) {
        head = node;
    }

    @Override
    public LinkedArrayNode<T> serGetTail() {
        return tail;
    }

    @Override
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
