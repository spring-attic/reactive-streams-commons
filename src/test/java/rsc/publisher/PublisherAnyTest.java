package rsc.publisher;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherAnyTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherAny<>(null, v -> true);
    }

    @Test(expected = NullPointerException.class)
    public void predicateNull() {
        new PublisherAny<>(null, null);
    }

    @Test
    public void normal() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();

        new PublisherAny<>(new PublisherRange(1, 10), v -> true).subscribe(ts);

        ts.assertValue(true)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>(0);

        new PublisherAny<>(new PublisherRange(1, 10), v -> true).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValue(true)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void none() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();

        new PublisherAny<>(new PublisherRange(1, 10), v -> false).subscribe(ts);

        ts.assertValue(false)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void noneBackpressured() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>(0);

        new PublisherAny<>(new PublisherRange(1, 10), v -> false).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValue(false)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void someMatch() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();

        new PublisherAny<>(new PublisherRange(1, 10), v -> v < 6).subscribe(ts);

        ts.assertValue(true)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void someMatchBackpressured() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>(0);

        new PublisherAny<>(new PublisherRange(1, 10), v -> v < 6).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValue(true)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void predicateThrows() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();

        new PublisherAny<>(new PublisherRange(1, 10), v -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

}
