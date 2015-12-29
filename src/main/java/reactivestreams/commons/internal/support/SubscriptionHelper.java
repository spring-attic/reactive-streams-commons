/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivestreams.commons.internal.support;

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
