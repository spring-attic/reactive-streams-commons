package reactivestreams.commons.publisher;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.publisher.PublisherAmb;
import reactivestreams.commons.publisher.PublisherError;
import reactivestreams.commons.publisher.PublisherNever;
import reactivestreams.commons.publisher.PublisherRange;
import reactivestreams.commons.subscriber.test.TestSubscriber;

import java.util.Arrays;

public class PublisherAmbTest {

    @Test(expected = NullPointerException.class)
    public void arrayNull() {
        new PublisherAmb<>((Publisher<Integer>[]) null);
    }

    @Test(expected = NullPointerException.class)
    public void iterableNull() {
        new PublisherAmb<>((Iterable<Publisher<Integer>>) null);
    }

    @Test
    public void firstWinner() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherAmb<>(new PublisherRange(1, 10), new PublisherRange(11, 10)).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void firstWinnerBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(5);

        new PublisherAmb<>(new PublisherRange(1, 10), new PublisherRange(11, 10)).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertNotComplete()
          .assertNoError();
    }

    @Test
    public void secondWinner() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherAmb<>(PublisherNever.instance(), new PublisherRange(11, 10)).subscribe(ts);

        ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void secondEmitsError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        RuntimeException ex = new RuntimeException("forced failure");

        new PublisherAmb<>(PublisherNever.instance(), new PublisherError<Integer>(ex)).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(ex);
    }

    @Test
    public void singleArrayNullSource() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherAmb<>((Publisher<Object>) null).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void arrayOneIsNullSource() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherAmb<>(PublisherNever.instance(), (Publisher<Object>) null, PublisherNever.instance()).subscribe
          (ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void singleIterableNullSource() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherAmb<>(Arrays.asList((Publisher<Object>) null)).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void iterableOneIsNullSource() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherAmb<>(Arrays.asList(PublisherNever.instance(), (Publisher<Object>) null, PublisherNever.instance
          ())).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

}
