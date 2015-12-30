package reactivestreams.commons;

import org.junit.Test;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherTakeTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherTake<>(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void numberIsInvalid() {
        new PublisherTake<>(PublisherNever.instance(), -1);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTake<>(new PublisherRange(1, 10), 5).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTake<>(new PublisherRange(1, 10), 5).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void takeZero() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTake<>(new PublisherRange(1, 10), 0).subscribe(ts);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }
}
