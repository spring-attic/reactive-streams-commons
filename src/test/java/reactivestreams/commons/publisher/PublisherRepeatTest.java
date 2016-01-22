package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherRepeatTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherRepeat<>(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void timesInvalid() {
        new PublisherRepeat<>(PublisherNever.instance(), -1);
    }

    @Test
    public void zeroRepeat() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRepeat<>(new PublisherRange(1, 10), 0).subscribe(ts);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void oneRepeat() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRepeat<>(new PublisherRange(1, 10), 1).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void oneRepeatBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherRepeat<>(new PublisherRange(1, 10), 1).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(3);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void twoRepeat() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRepeat<>(new PublisherRange(1, 5), 2).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void twoRepeatBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherRepeat<>(new PublisherRange(1, 5), 2).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(4);

        ts.assertValues(1, 2, 3, 4, 5, 1)
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);
        ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void repeatInfinite() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTake<>(new PublisherRepeat<>(new PublisherRange(1, 2)), 9).subscribe(ts);

        ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1)
          .assertComplete()
          .assertNoError();
    }
}
