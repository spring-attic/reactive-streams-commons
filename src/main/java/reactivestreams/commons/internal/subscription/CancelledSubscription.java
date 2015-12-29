package reactivestreams.commons.internal.subscription;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;

/**
 * A singleton Subscription that represents a cancelled subscription instance and should not be leaked to clients as it
 * represents a terminal state. <br> If algorithms need to hand out a subscription, replace this with {@link
 * EmptySubscription#INSTANCE} because there is no standard way to tell if a Subscription is cancelled or not
 * otherwise.
 */
public enum CancelledSubscription implements Subscription {
	INSTANCE;

	@Override
	public void request(long n) {
		// deliberately no op
	}

	@Override
	public void cancel() {
		// deliberately no op
	}

	/**
	 * Atomically sets a new Subscription in the field and cancels any previous Subscription or cancels the new
	 * Subscription if the field holds the cancelled instance.
	 *
	 * @param field
	 * @param next
	 *
	 * @return false if the field contains the cancelled Subscription
	 */
	public static boolean set(AtomicReference<Subscription> field, Subscription next) {
		for (; ; ) {
			Subscription current = field.get();
			if (current == INSTANCE) {
				if (next != null) {
					next.cancel();
				}
				return false;
			}
			if (field.compareAndSet(current, next)) {
				if (current != null) {
					current.cancel();
				}
				return true;
			}
		}
	}

	/**
	 * Atomically sets the first Subscription in field or returns false if the field was not null and not holding the
	 * cancelled instance.
	 *
	 * @param field
	 * @param first
	 *
	 * @return
	 */
	public static boolean setOnce(AtomicReference<Subscription> field, Subscription first) {
		Subscription current = field.get();
		if (current == INSTANCE) {
			if (first != null) {
				first.cancel();
			}
			return true;
		}
		if (current != null) {
			if (first != null) {
				first.cancel();
			}
			return false;
		}

		if (field.compareAndSet(null, first)) {
			return true;
		}

		if (first != null) {
			first.cancel();
		}
		current = field.get();
		return current == INSTANCE;
	}

	/**
	 * Atomically replaces the Subscription in field with the new Subscription but does not cancel the original
	 * Subscription.
	 *
	 * @param field
	 * @param next
	 *
	 * @return false if the field contains the cancelled Subscription
	 */
	public static boolean replace(AtomicReference<Subscription> field, Subscription next) {
		for (; ; ) {
			Subscription current = field.get();
			if (current == INSTANCE) {
				if (next != null) {
					next.cancel();
				}
				return false;
			}
			if (field.compareAndSet(current, next)) {
				return true;
			}
		}
	}

	/**
	 * Atomically replaces the Subscription in field with the cancelled instance and cancels the original Subscription.
	 *
	 * @param field
	 *
	 * @return true if the operation happened the first time
	 */
	public static boolean cancel(AtomicReference<Subscription> field) {
		Subscription current = field.get();
		if (current != INSTANCE) {
			current = field.getAndSet(INSTANCE);
			if (current != null && current != INSTANCE) {
				current.cancel();

				return true;
			}
		}
		return false;
	}

	/**
	 * Atomically sets a new Subscription in the field and cancels any previous Subscription or cancels the new
	 * Subscription if the field holds the cancelled instance.
	 *
	 * @param field
	 * @param updater
	 * @param instance
	 *
	 * @return false if the field contains the cancelled Subscription
	 */
	public static <T> boolean set(AtomicReferenceFieldUpdater<T, Subscription> updater, T instance, Subscription next) {
		for (; ; ) {
			Subscription current = updater.get(instance);
			if (current == INSTANCE) {
				if (next != null) {
					next.cancel();
				}
				return false;
			}
			if (updater.compareAndSet(instance, current, next)) {
				if (current != null) {
					current.cancel();
				}
				return true;
			}
		}
	}

	/**
	 * Atomically sets the first Subscription in field or returns false if the field was not null and not holding the
	 * cancelled instance.
	 *
	 * @param field
	 * @param updater
	 * @param instance
	 *
	 * @return
	 */
	public static <T> boolean setOnce(AtomicReferenceFieldUpdater<T, Subscription> updater,
			T instance,
			Subscription first) {
		Subscription current = updater.get(instance);
		if (current == INSTANCE) {
			if (first != null) {
				first.cancel();
			}
			return true;
		}
		if (current != null) {
			if (first != null) {
				first.cancel();
			}
			return false;
		}

		if (updater.compareAndSet(instance, null, first)) {
			return true;
		}

		if (first != null) {
			first.cancel();
		}
		current = updater.get(instance);
		return current == INSTANCE;
	}

	/**
	 * Atomically replaces the Subscription in field with the new Subscription but does not cancel the original
	 * Subscription.
	 *
	 * @param updater
	 * @param instance
	 * @param next
	 *
	 * @return false if the field contains the cancelled Subscription
	 */
	public static <T> boolean replace(AtomicReferenceFieldUpdater<T, Subscription> updater,
			T instance,
			Subscription next) {
		for (; ; ) {
			Subscription current = updater.get(instance);
			if (current == INSTANCE) {
				if (next != null) {
					next.cancel();
				}
				return false;
			}
			if (updater.compareAndSet(instance, current, next)) {
				return true;
			}
		}
	}

	/**
	 * Atomically replaces the Subscription in field with the cancelled instance and cancels the original Subscription.
	 *
	 * @param updater
	 * @param instance
	 *
	 * @return true if the operation happened the first time
	 */
	public static <T> boolean cancel(AtomicReferenceFieldUpdater<T, Subscription> updater, T instance) {
		Subscription current = updater.get(instance);
		if (current != INSTANCE) {
			current = updater.getAndSet(instance, INSTANCE);
			if (current != null && current != INSTANCE) {
				current.cancel();

				return true;
			}
		}
		return false;
	}

}
