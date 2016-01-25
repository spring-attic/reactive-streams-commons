package reactivestreams.commons.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;

/**
 * Utility methods to help working with Subscriptions and their methods.
 */
public enum SubscriptionHelper {
    ;

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
        new IllegalStateException("Subscription already set").printStackTrace();
    }

    public static void reportBadRequest(long n) {
        new IllegalArgumentException("request amount > 0 required but it was " + n).printStackTrace();
    }

    public static void reportMoreProduced() {
        new IllegalStateException("More produced than requested").printStackTrace();
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
        if (a != CancelledSubscription.INSTANCE) {
            a = field.getAndSet(instance, CancelledSubscription.INSTANCE);
            if (a != null && a != CancelledSubscription.INSTANCE) {
                a.cancel();
                return true;
            }
        }
        return false;
    }
    
    public static <F> boolean replace(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
        for (;;) {
            Subscription a = field.get(instance);
            if (a == CancelledSubscription.INSTANCE) {
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
            if (a == CancelledSubscription.INSTANCE) {
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
        if (a == CancelledSubscription.INSTANCE) {
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
        
        if (a == CancelledSubscription.INSTANCE) {
            s.cancel();
            return false;
        }
        
        s.cancel();
        reportSubscriptionSet();
        return false;
    }
}
