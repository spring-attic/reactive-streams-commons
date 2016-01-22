package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherWithLatestFromTest {


    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherWithLatestFrom<>(null, PublisherNever.instance(), (a, b) -> a);
    }

    @Test(expected = NullPointerException.class)
    public void otherNull() {
        new PublisherWithLatestFrom<>(PublisherNever.instance(), null, (a, b) -> a);
    }

    @Test(expected = NullPointerException.class)
    public void combinerNull() {
        new PublisherWithLatestFrom<>(PublisherNever.instance(), PublisherNever.instance(), null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherWithLatestFrom<>(new PublisherRange(1, 10), new PublisherJust<>(10), (a, b) -> a + b).subscribe
          (ts);

        ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherWithLatestFrom<>(new PublisherRange(1, 10), new PublisherJust<>(10), (a, b) -> a + b).subscribe
          (ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(2);

        ts.assertValues(11, 12)
          .assertNotComplete()
          .assertNoError();

        ts.request(5);

        ts.assertValues(11, 12, 13, 14, 15, 16, 17)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void otherIsNever() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherWithLatestFrom<>(new PublisherRange(1, 10), PublisherNever.<Integer>instance(), (a, b) -> a + b)
          .subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void otherIsEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherWithLatestFrom<>(new PublisherRange(1, 10), PublisherEmpty.<Integer>instance(), (a, b) -> a + b)
          .subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void combinerReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherWithLatestFrom<>(new PublisherRange(1, 10), new PublisherJust<>(10), (a, b) -> (Integer) null)
          .subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void combinerThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherWithLatestFrom<Integer, Integer, Integer>(
          new PublisherRange(1, 10), new PublisherJust<>(10),
          (a, b) -> {
              throw new RuntimeException("forced failure");
          }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }
}
