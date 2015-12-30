package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.publisher.*;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherSwitchIfEmptyTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherSwitchIfEmpty<>(null, PublisherNever.instance());
    }

    @Test(expected = NullPointerException.class)
    public void otherNull() {
        new PublisherSwitchIfEmpty<>(PublisherNever.instance(), null);
    }

    @Test
    public void nonEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSwitchIfEmpty<>(new PublisherRange(1, 5), new PublisherJust<>(10)).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void nonEmptyBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSwitchIfEmpty<>(new PublisherRange(1, 5), new PublisherJust<>(10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

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
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSwitchIfEmpty<>(PublisherEmpty.instance(), new PublisherJust<>(10)).subscribe(ts);

        ts.assertValue(10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void emptyBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSwitchIfEmpty<>(PublisherEmpty.instance(), new PublisherJust<>(10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValue(10)
          .assertComplete()
          .assertNoError();
    }

}
