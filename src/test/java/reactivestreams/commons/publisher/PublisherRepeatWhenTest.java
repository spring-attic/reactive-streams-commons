package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherRepeatWhenTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherRepeatWhen<>(null, v -> v);
    }

    @Test(expected = NullPointerException.class)
    public void whenFactoryNull() {
        new PublisherRepeatWhen<>(PublisherNever.instance(), null);
    }

    @Test
    public void coldRepeater() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRepeatWhen<>(new PublisherJust<>(1), v -> new PublisherRange(1, 10)).subscribe(ts);

        ts.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void coldRepeaterBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherRepeatWhen<>(new PublisherRange(1, 2), v -> new PublisherRange(1, 5)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2, 1)
          .assertNoError()
          .assertNotComplete();

        ts.request(5);

        ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1, 2)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void coldEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherRepeatWhen<>(new PublisherRange(1, 2), v -> PublisherEmpty.instance()).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void coldError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherRepeatWhen<>(new PublisherRange(1, 2), v -> new PublisherError<>(new RuntimeException("forced " +
          "failure"))).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();
    }

    @Test
    public void whenFactoryThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRepeatWhen<>(new PublisherRange(1, 2), v -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();

    }

    @Test
    public void whenFactoryReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRepeatWhen<>(new PublisherRange(1, 2), v -> null).subscribe(ts);

        ts.assertNoValues()
          .assertError(NullPointerException.class)
          .assertNotComplete();

    }

    @Test
    public void repeaterErrorsInResponse() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRepeatWhen<>(new PublisherRange(1, 2), v -> new PublisherMap<>(v, a -> {
            throw new RuntimeException("forced failure");
        })).subscribe(ts);

        ts.assertValues(1, 2)
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();

    }

    @Test
    public void retryAlways() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherRange(1, 2).repeatWhen(v -> v).subscribe(ts);
        
        ts.request(8);
        
        ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
        .assertNoError()
        .assertNotComplete();
    }

    @Test
    public void retryWithVolumeCondition() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherRange(1, 2).repeatWhen(v -> v.takeWhile(n -> n > 0)).subscribe(ts);

        ts.request(8);

        ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
        .assertNoError()
        .assertNotComplete();
    }

}
