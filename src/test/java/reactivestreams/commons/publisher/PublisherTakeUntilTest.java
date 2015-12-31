package reactivestreams.commons.publisher;

import org.junit.Test;

import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherTakeUntilTest {

    @Test(expected = NullPointerException.class)
    public void nullSource() {
        new PublisherTakeUntil<>(null, PublisherNever.instance());
    }

    @Test(expected = NullPointerException.class)
    public void nullOther() {
        new PublisherTakeUntil<>(PublisherNever.instance(), null);
    }

    @Test
    public void takeAll() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTakeUntil<>(new PublisherRange(1, 10), PublisherNever.instance()).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError()
        ;
    }

    @Test
    public void takeAllBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTakeUntil<>(new PublisherRange(1, 10), PublisherNever.instance()).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError()
        ;
    }

    @Test
    public void takeNone() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTakeUntil<>(new PublisherRange(1, 10), PublisherEmpty.instance()).subscribe(ts);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError()
        ;
    }

    @Test
    public void takeNoneBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTakeUntil<>(new PublisherRange(1, 10), PublisherEmpty.instance()).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void takeNoneOtherMany() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTakeUntil<>(new PublisherRange(1, 10), new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError()
        ;
    }

    @Test
    public void takeNoneBackpressuredOtherMany() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTakeUntil<>(new PublisherRange(1, 10), new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void otherSignalsError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTakeUntil<>(new PublisherRange(1, 10), new PublisherError<>(new RuntimeException("forced " +
          "failure"))).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
        ;
    }

    @Test
    public void otherSignalsErrorBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTakeUntil<>(new PublisherRange(1, 10), new PublisherError<>(new RuntimeException("forced " +
          "failure"))).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();
    }

}
