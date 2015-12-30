package reactivestreams.commons.publisher;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.publisher.PublisherJust;
import reactivestreams.commons.publisher.PublisherMap;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherMapTest {

    Publisher<Integer> just = new PublisherJust<>(1);

    @Test(expected = NullPointerException.class)
    public void nullSource() {
        new PublisherMap<Integer, Integer>(null, v -> v);
    }

    @Test(expected = NullPointerException.class)
    public void nullMapper() {
        new PublisherMap<Integer, Integer>(just, null);
    }

    @Test
    public void simpleMapping() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherMap<>(just, v -> v + 1).subscribe(ts);

        ts
          .assertNoError()
          .assertValue(2)
          .assertComplete()
        ;
    }

    @Test
    public void simpleMappingBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherMap<>(just, v -> v + 1).subscribe(ts);

        ts
          .assertNoError()
          .assertNoValues()
          .assertNotComplete()
        ;

        ts.request(1);

        ts.assertNoError()
          .assertValue(2)
          .assertComplete()
        ;
    }

    @Test
    public void mapperThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherMap<>(just, v -> {
            throw new RuntimeException("Forced failure");
        }).subscribe(ts);

        ts
          .assertError(RuntimeException.class)
          .assertErrorMessage("Forced failure")
          .assertNoValues()
          .assertNotComplete()
        ;
    }

    @Test
    public void mapperReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherMap<>(just, v -> null).subscribe(ts);

        ts
          .assertError(NullPointerException.class)
          .assertNoValues()
          .assertNotComplete()
        ;
    }
}
