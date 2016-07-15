package rsc.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.flow.Fuseable;
import rsc.flow.Trackable;
import rsc.util.UnsignalledExceptions;

/**
 * Utility methods to help working with Subscriptions and their methods.
 */
public enum SubscriptionHelper {
    ;

    /**
     * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onComplete.
     *
     * @param s
     */
    public static void complete(Subscriber<?> s) {
        s.onSubscribe(SubscriptionHelper.empty());
        s.onComplete();
    }

    /**
     * A singleton enumeration that represents a no-op Subscription instance that
     * can be freely given out to clients.
     * <p>
     * The enum also implements Fuseable.QueueSubscription so operators expecting a
     * QueueSubscription from a Fuseable source don't have to double-check their Subscription
     * received in onSubscribe.
     *
     * @return a singleton noop {@link Subscription}
     */
    public static Subscription empty(){
        return EmptySubscription.INSTANCE;
    }

    /**
     * A singleton Subscription that represents a cancelled subscription instance and should not be leaked to clients as it
     * represents a terminal state. <br> If algorithms need to hand out a subscription, replace this with {@code
     * EmptySubscription#INSTANCE} because there is no standard way to tell if a Subscription is cancelled or not
     * otherwise.
     * @return a singleton noop {@link Subscription}
     */
    public static Subscription cancelled(){
        return CancelledSubscription.INSTANCE;
    }

    /**
     * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onError with the
     * supplied error.
     *
     * @param s
     * @param e
     */
    public static void error(Subscriber<?> s, Throwable e) {
        s.onSubscribe(SubscriptionHelper.empty());
        s.onError(e);
    }

    public static boolean validate(Subscription current, Subscription next) {
        Objects.requireNonNull(next, "Subscription cannot be null");
        if (current != null) {
            next.cancel();
            reportSubscriptionSet();
            return false;
        }

        return true;
    }

    public static void reportSubscriptionSet() {
        UnsignalledExceptions.onErrorDropped(new IllegalStateException("Subscription already set"));
    }

    public static void reportBadRequest(long n) {
        UnsignalledExceptions.onErrorDropped(new IllegalArgumentException("request amount > 0 required but it was " + n));
    }

    public static void reportMoreProduced() {
        UnsignalledExceptions.onErrorDropped(new IllegalStateException("More produced than requested"));
    }

    public static boolean validate(long n) {
        if (n < 0) {
            reportBadRequest(n);
            return false;
        }
        return true;
    }
    
    /**
     * Atomically swaps in the single CancelledSubscription instance and returns true
     * if this was the first of such operation on the target field.
     * @param <F> the field type
     * @param field the field accessor
     * @param instance the parent instance of the field
     * @return true if the call triggered the cancellation of the underlying Subscription instance
     */
    public static <F> boolean terminate(AtomicReferenceFieldUpdater<F, Subscription> field, F instance) {
        Subscription a = field.get(instance);
        if (a != SubscriptionHelper.cancelled()) {
            a = field.getAndSet(instance, SubscriptionHelper.cancelled());
            if (a != null && a != SubscriptionHelper.cancelled()) {
                a.cancel();
                return true;
            }
        }
        return false;
    }

    public static <F> boolean replace(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
        for (;;) {
            Subscription a = field.get(instance);
            if (a == SubscriptionHelper.cancelled()) {
                s.cancel();
                return false;
            }
            if (field.compareAndSet(instance, a, s)) {
                return true;
            }
        }
    }

    public static <F> boolean set(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
        for (;;) {
            Subscription a = field.get(instance);
            if (a == SubscriptionHelper.cancelled()) {
                s.cancel();
                return false;
            }
            if (field.compareAndSet(instance, a, s)) {
                if (a != null) {
                    a.cancel();
                }
                return true;
            }
        }
    }

    /**
     * Sets the given subscription once and returns true if successful, false
     * if the field has a subscription already or has been cancelled.
     * @param <F> the instance type containing the field
     * @param field the field accessor
     * @param instance the parent instance
     * @param s the subscription to set once
     * @return true if successful, false if the target was not empty or has been cancelled
     */
    public static <F> boolean setOnce(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
        Subscription a = field.get(instance);
        if (a == SubscriptionHelper.cancelled()) {
            s.cancel();
            return false;
        }
        if (a != null) {
            reportSubscriptionSet();
            return false;
        }
        
        if (field.compareAndSet(instance, null, s)) {
            return true;
        }
        
        a = field.get(instance);
        
        if (a == SubscriptionHelper.cancelled()) {
            s.cancel();
            return false;
        }
        
        s.cancel();
        reportSubscriptionSet();
        return false;
    }
    
    /**
     * Returns the subscription as QueueSubscription if possible or null.
     * @param <T> the value type of the QueueSubscription.
     * @param s the source subscription to try to convert.
     * @return the QueueSubscription instance or null
     */
    @SuppressWarnings("unchecked")
    public static <T> Fuseable.QueueSubscription<T> as(Subscription s) {
        if (s instanceof Fuseable.QueueSubscription) {
            return (Fuseable.QueueSubscription<T>)s;
        }
        return null;
    }

    enum EmptySubscription implements Fuseable.QueueSubscription<Object> {
        INSTANCE;

        @Override
        public void request(long n) {
            // deliberately no op
        }

        @Override
        public void cancel() {
            // deliberately no op
        }

        @Override
        public Object poll() {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public void clear() {
            // deliberately no op
        }

        @Override
        public int requestFusion(int requestedMode) {
            return Fuseable.NONE; // can't enable fusion due to complete/error possibility
        }
    }

     enum CancelledSubscription implements Subscription, Trackable {
        INSTANCE;

        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public void request(long n) {
            // deliberately no op
        }

        @Override
        public void cancel() {
            // deliberately no op
        }


    }
}
