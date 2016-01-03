package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherFirstTest {

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherFirst<>(null);
    }

    @Test
    public void normal() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherFirst<>(new PublisherJust<>(1)).subscribe(ts);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherFirst<>(new PublisherJust<>(1)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void empty() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherFirst<>(PublisherEmpty.<Integer>instance()).subscribe(ts);

        ts.assertNoValues()
          .assertComplete();
    }

    @Test
    public void emptyDefault() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherFirst<>(PublisherEmpty.<Integer>instance()).subscribe(ts);

        ts.assertNoError()
          .assertComplete();
    }

    @Test
    public void multi() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherFirst<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertError(IndexOutOfBoundsException.class)
          .assertNotComplete();
    }

    @Test
    public void multiBackpressured() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherFirst<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertNoValues()
          .assertError(IndexOutOfBoundsException.class)
          .assertNotComplete();
    }

}
