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
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
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

        Px.range(1, 10).map(v -> v + 1).subscribe(ts);
        
        ts.assertValues(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusion() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
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

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        Px.just(1).hide().flatMap(w -> up.map(v -> v + 1)).subscribe(ts);
        
        up.onNext(1);
        
        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        up.onComplete();
        
        ts.assertValue(2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void mapFilter() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        Px.range(0, 1_000_000).map(v -> v + 1).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void mapFilterBackpressured() {
        TestSubscriber<Object> ts = new TestSubscriber<>(0);

        Px.range(0, 1_000_000).map(v -> v + 1).filter(v -> (v & 1) == 0).subscribe(ts);

        ts.assertNoError()
        .assertNoValues()
        .assertNotComplete()
        ;
        
        ts.request(250_000);
        
        ts.assertValueCount(250_000)
        .assertNoError()
        .assertNotComplete();

        ts.request(250_000);

        ts.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void hiddenMapFilter() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        Px.range(0, 1_000_000).hide().map(v -> v + 1).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void hiddenMapFilterBackpressured() {
        TestSubscriber<Object> ts = new TestSubscriber<>(0);

        Px.range(0, 1_000_000).hide().map(v -> v + 1).filter(v -> (v & 1) == 0).subscribe(ts);

        ts.assertNoError()
        .assertNoValues()
        .assertNotComplete()
        ;
        
        ts.request(250_000);
        
        ts.assertValueCount(250_000)
        .assertNoError()
        .assertNotComplete();

        ts.request(250_000);

        ts.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void hiddenMapHiddenFilterBackpressured() {
        TestSubscriber<Object> ts = new TestSubscriber<>(0);

        Px.range(0, 1_000_000).hide().map(v -> v + 1).hide().filter(v -> (v & 1) == 0).subscribe(ts);

        ts.assertNoError()
        .assertNoValues()
        .assertNotComplete()
        ;
        
        ts.request(250_000);
        
        ts.assertValueCount(250_000)
        .assertNoError()
        .assertNotComplete();

        ts.request(250_000);

        ts.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();
    }
}
