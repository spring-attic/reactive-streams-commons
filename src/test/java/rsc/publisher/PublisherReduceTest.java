package rsc.publisher;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherReduceTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherReduce<>(null, () -> 1, (a, b) -> (Integer) b);
    }

    @Test(expected = NullPointerException.class)
    public void supplierNull() {
        new PublisherReduce<>(PublisherNever.instance(), null, (a, b) -> b);
    }

    @Test(expected = NullPointerException.class)
    public void accumulatorNull() {
        new PublisherReduce<>(PublisherNever.instance(), () -> 1, null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherReduce<>(new PublisherRange(1, 10), () -> 0, (a, b) -> b).subscribe(ts);

        ts.assertValue(10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherReduce<>(new PublisherRange(1, 10), () -> 0, (a, b) -> b).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValue(10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void supplierThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherReduce<Integer, Integer>(new PublisherRange(1, 10), () -> {
            throw new RuntimeException("forced failure");
        }, (a, b) -> b).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void accumulatorThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherReduce<>(new PublisherRange(1, 10), () -> 0, (a, b) -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void supplierReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherReduce<Integer, Integer>(new PublisherRange(1, 10), () -> null, (a, b) -> b).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void accumulatorReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherReduce<>(new PublisherRange(1, 10), () -> 0, (a, b) -> null).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

}
