package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherHasElementsTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherHasElements<>(null);
    }

    @Test
    public void emptySource() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();

        new PublisherHasElements<>(PublisherEmpty.instance()).subscribe(ts);

        ts.assertValue(false)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void emptySourceBackpressured() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>(0);

        new PublisherHasElements<>(PublisherEmpty.instance()).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValue(false)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void nonEmptySource() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();

        new PublisherHasElements<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertValue(true)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void nonEmptySourceBackpressured() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>(0);

        new PublisherHasElements<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValue(true)
          .assertComplete()
          .assertNoError();
    }
}
