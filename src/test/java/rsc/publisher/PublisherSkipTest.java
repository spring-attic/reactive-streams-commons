package rsc.publisher;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherSkipTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherSkip<>(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void skipInvalid() {
        new PublisherSkip<>(PublisherNever.instance(), -1);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSkip<>(new PublisherRange(1, 10), 5).subscribe(ts);

        ts.assertValues(6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSkip<>(new PublisherRange(1, 10), 5).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(6, 7)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void skipAll() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSkip<>(new PublisherRange(1, 10), Long.MAX_VALUE).subscribe(ts);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }
}
