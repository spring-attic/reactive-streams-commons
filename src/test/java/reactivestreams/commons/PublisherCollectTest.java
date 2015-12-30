package reactivestreams.commons;

import org.junit.Test;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

import java.util.ArrayList;
import java.util.Arrays;

public class PublisherCollectTest {

    @Test(expected = NullPointerException.class)
    public void nullSource() {
        new PublisherCollect<>(null, () -> 1, (a, b) -> {
        });
    }

    @Test(expected = NullPointerException.class)
    public void nullSupplier() {
        new PublisherCollect<>(PublisherNever.instance(), null, (a, b) -> {
        });
    }

    @Test(expected = NullPointerException.class)
    public void nullAction() {
        new PublisherCollect<>(PublisherNever.instance(), () -> 1, null);
    }

    @Test
    public void normal() {
        TestSubscriber<ArrayList<Integer>> ts = new TestSubscriber<>();

        new PublisherCollect<>(new PublisherRange(1, 10), ArrayList<Integer>::new, (a, b) -> a.add(b)).subscribe(ts);

        ts.assertValue(new ArrayList<>(Arrays.<Integer>asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<ArrayList<Integer>> ts = new TestSubscriber<>(0);

        new PublisherCollect<>(new PublisherRange(1, 10), ArrayList<Integer>::new, (a, b) -> a.add(b)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValue(new ArrayList<>(Arrays.<Integer>asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void supplierThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherCollect<>(new PublisherRange(1, 10), () -> {
            throw new RuntimeException("forced failure");
        }, (a, b) -> {
        }).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();

    }

    @Test
    public void supplierReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherCollect<>(new PublisherRange(1, 10), () -> null, (a, b) -> {
        }).subscribe(ts);

        ts.assertNoValues()
          .assertError(NullPointerException.class)
          .assertNotComplete();
    }

    @Test
    public void actionThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherCollect<>(new PublisherRange(1, 10), () -> 1, (a, b) -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();

    }

}
