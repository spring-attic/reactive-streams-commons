package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherScanTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherScan<>(null, 1, (a, b) -> a);
    }

    @Test(expected = NullPointerException.class)
    public void initialValueNull() {
        new PublisherScan<>(PublisherNever.instance(), null, (a, b) -> a);
    }

    @Test(expected = NullPointerException.class)
    public void accumulatorNull() {
        new PublisherScan<>(PublisherNever.instance(), 1, null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherScan<>(new PublisherRange(1, 10), 0, (a, b) -> b).subscribe(ts);

        ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherScan<>(new PublisherRange(1, 10), 0, (a, b) -> b).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(0, 1)
          .assertNoError()
          .assertNotComplete();

        ts.request(8);

        ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void accumulatorThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherScan<>(new PublisherRange(1, 10), 0, (a, b) -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertValue(0)
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void accumulatorReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherScan<>(new PublisherRange(1, 10), 0, (a, b) -> null).subscribe(ts);

        ts.assertValue(0)
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }
}
