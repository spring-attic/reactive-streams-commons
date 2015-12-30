package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.publisher.PublisherNever;
import reactivestreams.commons.publisher.PublisherRange;
import reactivestreams.commons.publisher.PublisherSkipWhile;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherSkipWhileTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherSkipWhile<>(null, v -> true);
    }

    @Test(expected = NullPointerException.class)
    public void predicateNull() {
        new PublisherSkipWhile<>(PublisherNever.instance(), null);
    }

    @Test
    public void skipNone() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSkipWhile<>(new PublisherRange(1, 5), v -> false).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void skipNoneBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSkipWhile<>(new PublisherRange(1, 5), v -> false).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void skipSome() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSkipWhile<>(new PublisherRange(1, 5), v -> v < 3).subscribe(ts);

        ts.assertValues(3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void skipSomeBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSkipWhile<>(new PublisherRange(1, 5), v -> v < 3).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(3, 4)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void skipAll() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSkipWhile<>(new PublisherRange(1, 5), v -> true).subscribe(ts);
        ;

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void skipAllBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSkipWhile<>(new PublisherRange(1, 5), v -> true).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void predicateThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSkipWhile<>(new PublisherRange(1, 5), v -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");

    }

}
