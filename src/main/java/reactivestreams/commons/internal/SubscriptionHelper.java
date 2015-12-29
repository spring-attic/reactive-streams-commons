package reactivestreams.commons.internal;

import java.util.Objects;

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
}
