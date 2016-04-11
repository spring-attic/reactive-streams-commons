package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherMergeTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherMerge.class);
        
        ctb.addRef("sources", new Publisher[] { PublisherNever.instance()});
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addInt("maxConcurrency", 1, Integer.MAX_VALUE);
        ctb.addRef("mainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addRef("innerQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.mergeArray(Px.just(1), Px.range(2, 2), Px.fromArray(4, 5, 6).hide())
        .subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void mergeWithNoStackoverflow() {
        int n = 5000;
        
        Px<Integer> source = Px.just(1);
        
        Px<Integer> result = source;
        for (int i = 0; i < n; i++) {
            result = result.mergeWith(source);
        }
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        result.subscribe(ts);
        
        ts.assertValueCount(n + 1)
        .assertNoError()
        .assertComplete();
    }
}
