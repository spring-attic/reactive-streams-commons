package rsc.test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import org.reactivestreams.*;

import rsc.subscriber.*;
import rsc.util.SubscriptionHelper;
import rsc.flow.Fuseable;
import rsc.flow.Fuseable.*;

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
public class TestSubscriber<T> extends DeferredSubscriptionSubscriber<T, T> {

    final List<T> values;

    final List<Throwable> errors;

    final CountDownLatch cdl;

    int completions;

    int subscriptions;
    
    /** Set to true once to indicate a lack of Subscription has been detected and reported. */
    boolean subscriptionVerified;
    
    /** The timestamp of the last event. */
    long lastEvent;
    
    /** Incremented once a value has been added to values. */
    volatile int volatileSize;
    
    /** The fusion mode to request. */
    int requestedFusionMode = -1;
    
    /** The established fusion mode. */
    volatile int establishedFusionMode = -1;
    
    /** The fuseable QueueSubscription in case a fusion mode was specified. */
    QueueSubscription<T> qs;

    /**
     * Construct a TestSubscriber with no delegate Subscriber and requesting
     * Long.MAX_VALUE.
     */
    public TestSubscriber() {
        this(EmptySubscriber.instance(), Long.MAX_VALUE);
    }

    public TestSubscriber(Subscriber<? super T> delegate) {
        this(delegate, Long.MAX_VALUE);
    }

    /**
     * Construct a TestSubscriber with the specified initial request value
     * or 0 to not request anything upfront
     * @param initialRequest the initial request amount
     */
    public TestSubscriber(long initialRequest) {
        this(EmptySubscriber.instance(), initialRequest);
    }

    public TestSubscriber(Subscriber<? super T> delegate, long initialRequest) {
        super(delegate, initialRequest);
        this.values = new ArrayList<>();
        this.errors = new ArrayList<>();
        this.cdl = new CountDownLatch(1);
        this.lastEvent = System.currentTimeMillis();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void onSubscribe(Subscription s) {
        subscriptions++;
        int requestMode = requestedFusionMode;
        if (requestMode >= 0) {
            if (!setWithoutRequesting(s)) {
                if (!isCancelled()) {
                    errors.add(new IllegalStateException("Subscription already set: " + subscriptions));
                }
            } else {
                if (s instanceof QueueSubscription) {
                    this.qs = (QueueSubscription<T>)s;
                    
                    int m = qs.requestFusion(requestMode);
                    establishedFusionMode = m;
                    
                    if (m == Fuseable.SYNC) {
                        for (;;) {
                            T v = qs.poll();
                            if (v == null) {
                                onComplete();
                                break;
                            }
                            
                            onNext(v);
                        }
                    } else {
                        requestDeferred();
                    }
                } else {
                    requestDeferred();
                }
            }
        } else {
            if (!set(s)) {
                if (!isCancelled()) {
                    errors.add(new IllegalStateException("Subscription already set: " + subscriptions));
                }
            }
        }
    }

    @Override
    public void onNext(T t) {
        verifySubscription();
        if (establishedFusionMode == Fuseable.ASYNC) {
            for (;;) {
                t = qs.poll();
                if (t == null) {
                    break;
                }
                values.add(t);
                lastEvent = System.currentTimeMillis();
                volatileSize++;
                subscriber.onNext(t);
            }
        } else {
            values.add(t);
            lastEvent = System.currentTimeMillis();
            volatileSize++;
            subscriber.onNext(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        verifySubscription();
        errors.add(t);
        subscriber.onError(t);
        cdl.countDown();
    }

    @Override
    public void onComplete() {
        verifySubscription();
        completions++;
        subscriber.onComplete();
        cdl.countDown();
    }

    void verifySubscription() {
        if (!subscriptionVerified) {
            subscriptionVerified = true;
            if (subscriptions != 1) {
                errors.add(new IllegalStateException("Exactly 1 onSubscribe call expected but it was " + subscriptions));
            }
        }
    }
    
    @Override
    public void request(long n) {
        if (SubscriptionHelper.validate(n)) {
            if (establishedFusionMode != Fuseable.SYNC) {
                super.request(n);
            }
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
    final void assertionError(String message) {
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
        
        b.append("; values = ").append(volatileSize);
        
        AssertionError e = new AssertionError(b.toString());

        for (Throwable t : err) {
            e.addSuppressed(t);
        }

        throw e;
    }

    public final TestSubscriber<T> assertNoValues() {
        if (!values.isEmpty()) {
            assertionError("No values expected but received: [length = " + values.size() + "] " + values);
        }
        return this;
    }

    public final TestSubscriber<T> assertValueCount(int n) {
        int s = values.size();
        if (s != n) {
            assertionError("Different value count: expected = " + n + ", actual = " + s);
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
            assertionError("No values received");
        }
        if (s == 1) {
            if (!Objects.equals(value, values.get(0))) {
                assertionError("Values differ: expected = " + valueAndClass(value) + ", actual = " + valueAndClass(
                  values.get(0)));
            }
        }
        if (s > 1) {
            assertionError("More values received: expected = " + valueAndClass(value) + ", actual = " + "[" + s + "] " +
              "" + values);
        }
        return this;
    }

    @SafeVarargs
    public final TestSubscriber<T> assertValues(T... values) {
        int s = this.values.size();

        if (s != values.length) {
            assertionError("Different value count: expected = [length = " + values.length + "] " + Arrays.toString(
              values) + ", actual = [length = " + s + "] " + this.values);
        }
        for (int i = 0; i < s; i++) {
            T t1 = values[i];
            T t2 = this.values.get(i);

            if (!Objects.equals(t1, t2)) {
                assertionError("Values at " + i + " differ: expected = " + valueAndClass(t1) + ", actual = " +
                  valueAndClass(
                  t2));
            }
        }

        return this;
    }
    
    public final TestSubscriber<T> assertNoEvents() {
        return assertNoValues().assertNoError().assertNotComplete();
    }
    
    @SafeVarargs
    public final TestSubscriber<T> assertIncomplete(T... values) {
        return assertValues(values).assertNotComplete().assertNoError();
    }

    /**
     * Asserts that the TestSubscriber holds the specified values and has completed
     * without any error.
     * @param values the values to assert
     * @return this
     */
    @SafeVarargs
    public final TestSubscriber<T> assertResult(T... values) {
        return assertValues(values).assertComplete().assertNoError();
    }

    @SafeVarargs
    public final TestSubscriber<T> assertFailure(Throwable ex, T... values) {
        return assertValues(values).assertNotComplete().assertError(ex);
    }

    @SafeVarargs
    public final TestSubscriber<T> assertFailure(Class<? extends Throwable> ex, T... values) {
        return assertValues(values).assertNotComplete().assertError(ex);
    }

    @SafeVarargs
    public final TestSubscriber<T> assertFailureMessage(Class<? extends Throwable> ex, String message, T... values) {
        return assertValues(values).assertNotComplete().assertError(ex).assertErrorMessage(message);
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
                      t1));
                }
                i++;
            } else if (n1 && !n2) {
                assertionError("Actual contains more elements" + values);
                break;
            } else if (!n1 && n2) {
                assertionError("Actual contains fewer elements: " + values);
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
            assertionError("Not completed");
        }
        if (c > 1) {
            assertionError("Multiple completions: " + c);
        }

        return this;
    }

    public final TestSubscriber<T> assertNotComplete() {
        int c = completions;
        if (c == 1) {
            assertionError("Completed");
        }

        if (c > 1) {
            assertionError("Multiple completions: " + c);
        }

        return this;
    }

    public final TestSubscriber<T> assertTerminated() {
        if (cdl.getCount() != 0) {
            assertionError("Not terminated");
        }
        return this;
    }

    public final TestSubscriber<T> assertNotTerminated() {
        if (cdl.getCount() == 0) {
            assertionError("Terminated");
        }
        return this;
    }

    public final TestSubscriber<T> assertNoError() {
        int s = errors.size();
        if (s == 1) {
            assertionError("Error present: " + valueAndClass(errors.get(0)));
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s);
        }
        return this;
    }

    public final TestSubscriber<T> assertError(Throwable e) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error");
        }
        if (s == 1) {
            if (!Objects.equals(e, errors.get(0))) {
                assertionError("Errors differ: expected = " + valueAndClass(e) + ", actual = " + valueAndClass(errors
                  .get(
                  0)));
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s);
        }

        return this;
    }

    public final TestSubscriber<T> assertError(Class<? extends Throwable> clazz) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error");
        }
        if (s == 1) {
            Throwable e = errors.get(0);
            if (!clazz.isInstance(e)) {
                assertionError("Error class incompatible: expected = " + clazz.getSimpleName() + ", actual = " +
                  valueAndClass(
                  e));
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorMessage(String message) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error");
        }
        if (s == 1) {
            if (!Objects.equals(message,
              errors.get(0)
                .getMessage())) {
                assertionError("Error class incompatible: expected = \"" + message + "\", actual = \"" + errors.get(0)
                    .getMessage() + "\"");
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorCause(Throwable e) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error");
        }
        if (s == 1) {
            Throwable cause = errors.get(0)
              .getCause();
            if (!Objects.equals(e, cause)) {
                assertionError("Errors differ: expected = " + valueAndClass(e) + ", actual = " + valueAndClass(cause));
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorCause(Class<? extends Throwable> clazz) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error");
        }
        if (s == 1) {
            Throwable cause = errors.get(0).getCause();
            if (!clazz.isInstance(cause)) {
                assertionError("Error class incompatible: expected = " + clazz.getSimpleName() + ", actual = " + valueAndClass(
                  cause));
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s);
        }

        return this;
    }

    public final TestSubscriber<T> assertErrorCauseMessage(String message) {
        int s = errors.size();
        if (s == 0) {
            assertionError("No error");
        }
        if (s == 1) {
            Throwable cause = errors.get(0).getCause();
            if (cause == null) {
                assertionError("No cause to " + errors.get(0));
            } else
            if (!Objects.equals(message, cause.getMessage())) {
                assertionError("Error messages differ: expected = " + message + ", actual = " + cause.getMessage());
            }
        }
        if (s > 1) {
            assertionError("Multiple errors: " + s);
        }

        return this;
    }

    public final TestSubscriber<T> assertSubscribed() {
        int s = subscriptions;

        if (s == 0) {
            assertionError("OnSubscribe not called");
        }
        if (s > 1) {
            assertionError("OnSubscribe called multiple times: " + s);
        }

        return this;
    }

    public final TestSubscriber<T> assertNotSubscribed() {
        int s = subscriptions;

        if (s == 1) {
            assertionError("OnSubscribe called once");
        }
        if (s > 1) {
            assertionError("OnSubscribe called multiple times: " + s);
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
            assertionError("Wait interrupted");
        }
    }

    public final boolean await(long timeout, TimeUnit unit) {
        if (cdl.getCount() == 0) {
            return true;
        }
        try {
            return cdl.await(timeout, unit);
        } catch (InterruptedException ex) {
            assertionError("Wait interrupted");
            return false;
        }
    }

    /**
     * Blocking method that waits until {@code n} next values have been received.
     * @param n the value count to assert
     * @return this
     */
    public final TestSubscriber<T> awaitAndAssertValueCount(final int n) {
        long defaultTimeout = 5000;
        long delta = System.currentTimeMillis() + defaultTimeout;
        for(;;){
            if (isCancelled()) {
                assertionError("Cancelled while waiting for " + n + " values");
                return this;
            }
            int size = volatileSize;
            if (size >= n) {
                return this;
            }
            if (System.currentTimeMillis() > delta) {
                assertionError("Timeout while waiting for "+n+" values, current: " + size);
                return this;
            }
            LockSupport.parkNanos(1_000L);
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
    
    /**
     * Returns the number of received events (volatile read).
     * @return the number of received events
     */
    public final int received() {
        return volatileSize;
    }
    
    /**
     * Setup what fusion mode should be requested from the incomining
     * Subscription if it happens to be QueueSubscription
     * @param requestMode the mode to request, see Fuseable constants
     * @return this
     */
    public final TestSubscriber<T> requestedFusionMode(int requestMode) {
        this.requestedFusionMode = requestMode;
        return this;
    }
    
    /**
     * Returns the established fusion mode or -1 if it was not enabled
     * @return the fusion mode, see Fuseable constants
     */
    public final int establishedFusionMode() {
        return establishedFusionMode;
    }
    
    String fusionModeName(int mode) {
        switch (mode) {
        case -1: return "Disabled";
        case Fuseable.NONE: return "None";
        case Fuseable.SYNC: return "Sync";
        case Fuseable.ASYNC: return "Async";
        default: return "Unknown(" + mode + ")";
        }
    }
    
    public final TestSubscriber<T> assertFusionMode(int expectedMode) {
        if (establishedFusionMode != expectedMode) {
            assertionError("Wrong fusion mode: expected: "
                    + fusionModeName(expectedMode) + ", actual: " 
                    + fusionModeName(establishedFusionMode));
        }
        return this;
    }
    
    /**
     * Assert that the fusion mode was granted.
     * @return this
     */
    public final TestSubscriber<T> assertFusionEnabled() {
        if (establishedFusionMode != Fuseable.SYNC
                && establishedFusionMode != Fuseable.ASYNC) {
            assertionError("Fusion was not enabled");
        }
        return this;
    }

    /**
     * Assert that the fusion mode was granted.
     * @return this
     */
    public final TestSubscriber<T> assertFusionRejected() {
        if (establishedFusionMode != Fuseable.NONE) {
            assertionError("Fusion was granted");
        }
        return this;
    }

    /**
     * Assert that the upstream was a Fuseable source.
     * @return this
     */
    public final TestSubscriber<T> assertFuseableSource() {
        if (qs == null) {
            assertionError("Upstream was not Fuseable");
        }
        return this;
    }
    
    /**
     * Assert that the upstream was not a Fuseable source.
     * @return this
     */
    public final TestSubscriber<T> assertNonFuseableSource() {
        if (qs != null) {
            assertionError("Upstream was Fuseable");
        }
        return this;
    }
    
    /**
     * Asserts that the source terminates within the specified amount of
     * time or else throws an AssertionError with the received event
     * count and how long the last event was seen.
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @return this
     */
    public final TestSubscriber<T> assertTerminated(long timeout, TimeUnit unit) {
        long ts = System.currentTimeMillis();
        if (!await(timeout, unit)) {
            cancel();
            assertionError("TestSubscriber timed out. Received: " + volatileSize + ", last event: " + (ts - lastEvent) + " ms ago");
        }
        return this;
    }
    
    /**
     * Assert that all the values of this TestSubscriber are in the provided set.
     * @param set the set to use for checking all the values are in there
     * @return this
     */
    public final TestSubscriber<T> assertValueSet(Set<T> set) {
        int i = 0;
        for (T t : values) {
            if (!set.contains(t)) {
                assertionError("The value " + valueAndClass(t) + " at index " + i + " is not in the set");
            }
            i++;
        }
        return this;
    }
}
