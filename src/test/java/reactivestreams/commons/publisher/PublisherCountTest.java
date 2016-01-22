package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherCountTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherCount<>(null);
    }

    public void normal() {
        TestSubscriber<Long> ts = new TestSubscriber<>();

        new PublisherCount<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertValue(10L)
          .assertComplete()
          .assertNoError();
    }

    public void normalBackpressured() {
        TestSubscriber<Long> ts = new TestSubscriber<>(0);

        new PublisherCount<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(2);

        ts.assertValue(10L)
          .assertComplete()
          .assertNoError();
    }

}
