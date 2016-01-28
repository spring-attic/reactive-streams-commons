package reactivestreams.commons.publisher;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.processor.UnicastProcessor;
import reactivestreams.commons.test.TestSubscriber;

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
    
    @Test
    public void syncFusion() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        PublisherBase.range(1, 10).map(v -> v + 1).subscribe(ts);
        
        ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusion() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = UnicastProcessor.create(new ConcurrentLinkedQueue<>());
        
        up.map(v -> v + 1).subscribe(ts);
        
        for (int i = 1; i < 11; i++) {
            up.onNext(i);
        }
        up.onComplete();
        
        ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusionBackpressured() {
        TestSubscriber<Object> ts = new TestSubscriber<>(1);

        UnicastProcessor<Integer> up = UnicastProcessor.create(new ConcurrentLinkedQueue<>());
        
        PublisherBase.just(1).hide().flatMap(w -> up.map(v -> v + 1)).subscribe(ts);
        
        up.onNext(1);
        
        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        up.onComplete();
        
        ts.assertValue(2)
        .assertNoError()
        .assertComplete();
    }
}
