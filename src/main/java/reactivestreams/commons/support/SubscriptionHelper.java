package reactivestreams.commons.support;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;

import reactivestreams.commons.subscription.CancelledSubscription;

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
}
