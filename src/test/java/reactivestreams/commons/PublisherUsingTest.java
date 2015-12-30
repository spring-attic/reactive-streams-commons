package reactivestreams.commons;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.processor.SimpleProcessor;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PublisherUsingTest {

    @Test(expected = NullPointerException.class)
    public void resourceSupplierNull() {
        new PublisherUsing<>(null, r -> PublisherEmpty.instance(), r -> {
        }, false);
    }

    @Test(expected = NullPointerException.class)
    public void sourceFactoryNull() {
        new PublisherUsing<>(() -> 1, null, r -> {
        }, false);
    }

    @Test(expected = NullPointerException.class)
    public void resourceCleanupNull() {
        new PublisherUsing<>(() -> 1, r -> PublisherEmpty.instance(), null, false);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicInteger cleanup = new AtomicInteger();

        new PublisherUsing<>(() -> 1, r -> new PublisherRange(r, 10), cleanup::set, false).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();

        Assert.assertEquals(1, cleanup.get());
    }

    @Test
    public void normalEager() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicInteger cleanup = new AtomicInteger();

        new PublisherUsing<>(() -> 1, r -> new PublisherRange(r, 10), cleanup::set, true).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();

        Assert.assertEquals(1, cleanup.get());
    }

    void checkCleanupExecutionTime(boolean eager, boolean fail) {
        AtomicInteger cleanup = new AtomicInteger();
        AtomicBoolean before = new AtomicBoolean();

        Subscriber<Integer> delegate = new Subscriber<Integer>() {

            @Override
            public void onSubscribe(Subscription s) {

            }

            @Override
            public void onNext(Integer t) {

            }

            @Override
            public void onError(Throwable t) {
                before.set(cleanup.get() != 0);
            }

            @Override
            public void onComplete() {
                before.set(cleanup.get() != 0);
            }
        };

        TestSubscriber<Integer> ts = new TestSubscriber<>(delegate);


        new PublisherUsing<>(() -> 1, r -> {
            if (fail) {
                return new PublisherError<>(new RuntimeException("forced failure"));
            }
            return new PublisherRange(r, 10);
        }, cleanup::set, eager).subscribe(ts);

        if (fail) {
            ts.assertNoValues()
              .assertError(RuntimeException.class)
              .assertNotComplete()
              .assertErrorMessage("forced failure");
        } else {
            ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
              .assertComplete()
              .assertNoError();
        }

        Assert.assertEquals(1, cleanup.get());
        Assert.assertEquals(eager, before.get());
    }

    @Test
    public void checkNonEager() {
        checkCleanupExecutionTime(false, false);
    }


    @Test
    public void checkEager() {
        checkCleanupExecutionTime(true, false);
    }

    @Test
    public void checkErrorNonEager() {
        checkCleanupExecutionTime(false, true);
    }


    @Test
    public void checkErrorEager() {
        checkCleanupExecutionTime(true, true);
    }

    @Test
    public void resourceThrowsEager() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        AtomicInteger cleanup = new AtomicInteger();

        new PublisherUsing<>(() -> {
            throw new RuntimeException("forced failure");
        }, r -> new PublisherRange(1, 10), cleanup::set, false).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");

        Assert.assertEquals(0, cleanup.get());
    }

    @Test
    public void factoryThrowsEager() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        AtomicInteger cleanup = new AtomicInteger();

        new PublisherUsing<>(() -> 1, r -> {
            throw new RuntimeException("forced failure");
        }, cleanup::set, false).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");

        Assert.assertEquals(1, cleanup.get());
    }

    @Test
    public void factoryReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        AtomicInteger cleanup = new AtomicInteger();

        new PublisherUsing<Integer, Integer>(() -> 1, r -> null, cleanup::set, false).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);

        Assert.assertEquals(1, cleanup.get());
    }

    @Test
    public void subscriberCancels() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicInteger cleanup = new AtomicInteger();

        SimpleProcessor<Integer> tp = new SimpleProcessor<>();

        new PublisherUsing<>(() -> 1, r -> tp, cleanup::set, true).subscribe(ts);

        Assert.assertTrue("No subscriber?", tp.hasSubscribers());

        tp.onNext(1);

        ts.assertValue(1)
          .assertNotComplete()
          .assertNoError();

        ts.cancel();

        tp.onNext(2);

        ts.assertValue(1)
          .assertNotComplete()
          .assertNoError();

        Assert.assertFalse("Has subscriber?", tp.hasSubscribers());

        Assert.assertEquals(1, cleanup.get());
    }

}
