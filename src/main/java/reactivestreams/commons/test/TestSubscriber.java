package reactivestreams.commons.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.subscriber.EmptySubscriber;
import reactivestreams.commons.subscriber.SubscriberDeferredSubscription;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * A Subscriber implementation that hosts assertion tests for its state and allows asynchronous cancellation and
 * requesting.
 * <p>
 * <p>
 * You can extend this class but only the onNext, onError and onComplete can be overridden.
 * <p>
 * You can call {@link #request(long)} and {@link #cancel()} from any thread or from within the overridable methods but
 * you should avoid calling the assertXXX methods asynchronously.
 *
 * @param <T> the value type.
 */
public class TestSubscriber<T> extends SubscriberDeferredSubscription<T, T> {

    final List<T> values;

    final List<Throwable> errors;

    final CountDownLatch cdl;

    int completions;

    int subscriptions;

    public TestSubscriber() {
        this(EmptySubscriber.instance(), Long.MAX_VALUE);
    }

    public TestSubscriber(Subscriber<? super T> delegate) {
        this(delegate, Long.MAX_VALUE);
    }

    public TestSubscriber(long initialRequest) {
        this(EmptySubscriber.instance(), initialRequest);
    }

    public TestSubscriber(Subscriber<? super T> delegate, long initialRequest) {
        super(delegate, initialRequest);
        this.values = new ArrayList<>();
        this.errors = new ArrayList<>();
        this.cdl = new CountDownLatch(1);
    }

    @Override
    public final void onSubscribe(Subscription s) {
        subscriptions++;
        if (!set(s)) {
            if (!isCancelled()) {
                errors.add(new IllegalStateException("subscription already set"));
            }
        }
    }

    @Override
    public void onNext(T t) {
        values.add(t);
        subscriber.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        errors.add(t);
        subscriber.onError(t);
        cdl.countDown();
    }

    @Override
    public void onComplete() {
        completions++;
        subscriber.onComplete();
        cdl.countDown();
    }

    @Override
    public void request(long n) {
        if (SubscriptionHelper.validate(n)) {
            super.request(n);
        }
    }

    /**
     * Prepares and throws an AssertionError exception based on the message, cause, the active state and the potential
     * errors so far.
     *
     * @param message the message
     * @param cause   the optional Throwable cause
     * @throws AssertionError as expected
     */
    final void assertionError(String message, Throwable cause) {
        StringBuilder b = new StringBuilder();

        if (cdl.getCount() != 0) {
            b.append("(active) ");
        }
        b.append(message);

        List<Throwable> err = errors;
        if (!err.isEmpty()) {
            b.append(" (+ ")
              .append(err.size())
              .append(" errors)");
        }
        AssertionError e = new AssertionError(b.toString(), cause);

        for (Throwable t : err) {
            e.addSuppressed(t);
        }

        throw e;
    }

    public final TestSubscriber<T> assertNoValues() {
        if (!values.isEmpty()) {
            assertionError("No values expected but received: [length = " + values.size() + "] " + values, null);
        }
        return this;
    }

    public final TestSubscriber<T> assertValueCount(int n) {
        int s = values.size();
        if (s != n) {
            assertionError("Different value count: expected = " + n + ", actual = " + s, null);
        }
        return this;
    }

    final String valueAndClass(Object o) {
        if (o == null) {
            return null;
        }
        return o + " (" + o.getClass()
          .getSimpleName() + ")";
    }

    public final TestSubscriber<T> assertValue(T value) {
        int s = values.size();
        if (s == 0) {
            assertionError("No values received", null);
        }
        if (s == 1) {
            if (!Objects.equals(value, values.get(0))) {
                assertionError("Values differ: expected = " + valueAndClass(value) + ", actual = " + valueAndClass(
                  values.get(0)), null);
            }
        }
        if (s > 1) {
            assertionError("More values received: expected = " + valueAndClass(value) + ", actual = " + "[" + s + "] " +
              "" + values,
              null);
        }
        return this;
    }

    @SafeVarargs
    public final TestSubscriber<T> assertValues(T... values) {
        int s = this.values.size();

        if (s != values.length) {
            assertionError("Different value count: expected = [length = " + values.length + "] " + Arrays.toString(
              values) + ", actual = [length = " + s + "] " + this.values, null);
        }
        for (int i = 0; i < s; i++) {
            T t1 = values[i];
            T t2 = this.values.get(i);

            if (!Objects.equals(t1, t2)) {
                assertionError("Values at " + i + " differ: expected = " + valueAndClass(t1) + ", actual = " +
                  valueAndClass(
                  t2), null);
            }
        }

        return this;
    }

    public final TestSubscriber<T> assertValueSequence(Iterable<? extends T> expectedSequence) {

        Iterator<T> actual = values.iterator();
        Iterator<? extends T> expected = expectedSequence.iterator();

        int i = 0;

        for (; ; ) {
            boolean n1 = actual.hasNext();
            boolean n2 = expected.hasNext();

            if (n1 && n2) {
                T t1 = actual.next();
                T t2 = expected.next();

                if (!Objects.equals(t1, t2)) {
                    assertionError("The " + i + " th elements differ: expected = " + valueAndClass(t2) + ", actual ="
                      + valueAndClass(
                      t1), null);
                }
                i++;
            } else if (n1 && !n2) {
                assertionError("Actual contains more elements" + values, null);
                break;
            } else if (!n1 && n2) {
                assertionError("Actual contains fewer elements: " + values, null);
                break;
            } else {
                break;
            }

        }

        return this;
    }

    public final TestSubscriber<T> assertComplete() {
        int c = completions;
        if (c == 0) {
            assertionError("Not completed", null);
        }
        if (c > 1) {
            assertionError("Multiple completions: " + c, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertNotComplete() {
        int c = completions;
        if (c == 1) {
            assertionError("Completed", null);
        }

        if (c > 1) {
            assertionError("Multiple completions: " + c, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertTerminated() {
        if (cdl.getCount() != 0) {
            assertionError("Not terminated", null);
        }
        return this;
    }

    public final TestSubscriber<T> assertNotTerminated() {
        if (cdl.getCount() == 0) {
            assertionError("Terminated", null);
        }
        return this;
    }

    public final TestSubscriber<T> assertNoError() {
        int s = errors.size();
        if (s == 1) {
            assertionError("Error present: " + valueAndClass(errors.get(0)), null);
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s, null);
        }
        return this;
    }

    public final TestSubscriber<T> assertError(Throwable e) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error", null);
        }
        if (s == 1) {
            if (!Objects.equals(e, errors.get(0))) {
                assertionError("Errors differ: expected = " + valueAndClass(e) + ", actual = " + valueAndClass(errors
                  .get(
                  0)), null);
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertError(Class<? extends Throwable> clazz) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error", null);
        }
        if (s == 1) {
            Throwable e = errors.get(0);
            if (!clazz.isInstance(e)) {
                assertionError("Error class incompatible: expected = " + clazz.getSimpleName() + ", actual = " +
                  valueAndClass(
                  e), null);
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorMessage(String message) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error", null);
        }
        if (s == 1) {
            if (!Objects.equals(message,
              errors.get(0)
                .getMessage())) {
                assertionError("Error class incompatible: expected = \"" + message + "\", actual = \"" + errors.get(0)
                    .getMessage() + "\"",
                  null);
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorCause(Throwable e) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error", null);
        }
        if (s == 1) {
            Throwable cause = errors.get(0)
              .getCause();
            if (!Objects.equals(e, cause)) {
                assertionError("Errors differ: expected = " + valueAndClass(e) + ", actual = " + valueAndClass(cause),
                  null);
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorCause(Class<? extends Throwable> clazz) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error", null);
        }
        if (s == 1) {
            Throwable cause = errors.get(0).getCause();
            if (!clazz.isInstance(cause)) {
                assertionError("Error class incompatible: expected = " + clazz.getSimpleName() + ", actual = " + valueAndClass(
                  cause), null);
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorCauseMessage(String message) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error", null);
        }
        if (s == 1) {
            Throwable cause = errors.get(0).getCause();
            if (cause == null) {
                assertionError("No cause to " + errors.get(0), errors.get(0));
            } else
            if (!Objects.equals(message, cause.getMessage())) {
                assertionError("Error messages differ: expected = " + message + ", actual = " + cause.getMessage(), cause);
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertSubscribed() {
        int s = subscriptions;

        if (s == 0) {
            assertionError("OnSubscribe not called", null);
        }
        if (s > 1) {
            assertionError("OnSubscribe called multiple times: " + s, null);
        }

        return this;
    }

    public final TestSubscriber<T> assertNotSubscribed() {
        int s = subscriptions;

        if (s == 1) {
            assertionError("OnSubscribe called once", null);
        }
        if (s > 1) {
            assertionError("OnSubscribe called multiple times: " + s, null);
        }

        return this;
    }

    public final void await() {
        if (cdl.getCount() == 0) {
            return;
        }
        try {
            cdl.await();
        } catch (InterruptedException ex) {
            assertionError("Wait interrupted", ex);
        }
    }

    public final boolean await(long timeout, TimeUnit unit) {
        if (cdl.getCount() == 0) {
            return true;
        }
        try {
            return cdl.await(timeout, unit);
        } catch (InterruptedException ex) {
            assertionError("Wait interrupted", ex);
            return false;
        }
    }

    public final List<T> values() {
        return values;
    }

    public final List<Throwable> errors() {
        return errors;
    }

    public final int completions() {
        return completions;
    }

    public final int subscriptions() {
        return subscriptions;
    }
}
