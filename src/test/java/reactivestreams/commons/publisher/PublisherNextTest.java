package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherNextTest {

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherNext<>(null);
    }

    @Test
    public void normal() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherNext<>(new PublisherJust<>(1)).subscribe(ts);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherNext<>(new PublisherJust<>(1)).subscribe(ts);

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

        new PublisherNext<>(PublisherEmpty.<Integer>instance()).subscribe(ts);

        ts.assertNoValues()
          .assertComplete();
    }

    @Test
    public void emptyDefault() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherNext<>(PublisherEmpty.<Integer>instance()).subscribe(ts);

        ts.assertNoError()
          .assertComplete();
    }

    @Test
    public void multi() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherNext<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertValue(1)
          .assertComplete();
    }

    @Test
    public void multiBackpressured() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherNext<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertComplete();
    }

}
