package reactivestreams.commons.publisher;

import org.junit.Assert;
import org.junit.Test;
import reactivestreams.commons.publisher.PublisherEmpty;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherEmptyTest {

    @Test
    public void singleInstance() {
        Assert.assertSame(PublisherEmpty.instance(), PublisherEmpty.instance());
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        PublisherEmpty.<Integer>instance().subscribe(ts);

        ts
          .assertSubscribed()
          .assertNoValues()
          .assertNoError()
          .assertComplete();
    }
}
