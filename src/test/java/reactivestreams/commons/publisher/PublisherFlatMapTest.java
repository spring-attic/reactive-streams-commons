package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.*;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherFlatMapTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherFlatMap.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("mapper", (Function<Object, Publisher<Object>>)v -> PublisherNever.instance());
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addInt("maxConcurrency", 1, Integer.MAX_VALUE);
        ctb.addRef("mainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addRef("innerQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherRange(v, 2)).subscribe(ts);
        
        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherRange(v, 2)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1000);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertNotComplete();

        ts.request(1000);

        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void mainError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherError<Integer>(new RuntimeException("Forced failure"))
        .flatMap(v -> new PublisherJust<>(v)).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
    }

    @Test
    public void innerError() {
        TestSubscriber<Object> ts = new TestSubscriber<>(0);

        new PublisherJust<>(1).flatMap(v -> new PublisherError<>(new RuntimeException("Forced failure"))).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
    }

    @Test
    public void normalQueueOpt() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherArray<>(v, v + 1)).subscribe(ts);
        
        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalQueueOptBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherArray<>(v, v + 1)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1000);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertNotComplete();

        ts.request(1000);

        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void nullValue() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherArray<>((Integer)null)).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mainEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        PublisherBase.<Integer>empty().flatMap(v -> new PublisherJust<>(v)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void innerEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        PublisherBase.range(1, 1000).flatMap(v -> PublisherBase.<Integer>empty()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

}
