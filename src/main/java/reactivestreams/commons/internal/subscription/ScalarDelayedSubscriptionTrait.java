package reactivestreams.commons.internal.subscription;

import java.util.Objects;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.SubscriptionHelper;

/**
 * Interface with default methods to handle a scalar delayed value emission.
 */
public interface ScalarDelayedSubscriptionTrait<T> extends Subscription {

	static final int SDS_NO_REQUEST_NO_VALUE   = 0;
	static final int SDS_NO_REQUEST_HAS_VALUE  = 1;
	static final int SDS_HAS_REQUEST_NO_VALUE  = 2;
	static final int SDS_HAS_REQUEST_HAS_VALUE = 3;

	int sdsGetState();

	void sdsSetState(int updated);

	boolean sdsCasState(int expected, int updated);

	T sdsGetValue();

	void sdsSetValue(T value);

	Subscriber<? super T> sdsGetSubscriber();

	@Override
	default void request(long n) {
		if (SubscriptionHelper.validate(n)) {
			for (; ; ) {
				int s = sdsGetState();
				if (s == SDS_HAS_REQUEST_NO_VALUE || s == SDS_HAS_REQUEST_HAS_VALUE) {
					return;
				}
				if (s == SDS_NO_REQUEST_HAS_VALUE) {
					if (sdsCasState(SDS_NO_REQUEST_HAS_VALUE, SDS_HAS_REQUEST_HAS_VALUE)) {
						Subscriber<? super T> a = sdsGetSubscriber();
						a.onNext(sdsGetValue());
						a.onComplete();
					}
					return;
				}
				if (sdsCasState(SDS_NO_REQUEST_NO_VALUE, SDS_HAS_REQUEST_NO_VALUE)) {
					return;
				}
			}
		}
	}

	default void sdsSet(T value) {
		Objects.requireNonNull(value);
		for (; ; ) {
			int s = sdsGetState();
			if (s == SDS_NO_REQUEST_HAS_VALUE || s == SDS_HAS_REQUEST_HAS_VALUE) {
				return;
			}
			if (s == SDS_HAS_REQUEST_NO_VALUE) {
				if (sdsCasState(SDS_HAS_REQUEST_NO_VALUE, SDS_HAS_REQUEST_HAS_VALUE)) {
					Subscriber<? super T> a = sdsGetSubscriber();
					a.onNext(value);
					a.onComplete();
				}
				return;
			}
			sdsSetValue(value);
			if (sdsCasState(SDS_NO_REQUEST_NO_VALUE, SDS_NO_REQUEST_HAS_VALUE)) {
				return;
			}
		}
	}

	@Override
	default void cancel() {
		sdsSetState(SDS_HAS_REQUEST_HAS_VALUE);
	}

	default boolean isCancelled() {
		return sdsGetState() == SDS_HAS_REQUEST_HAS_VALUE;
	}
}
