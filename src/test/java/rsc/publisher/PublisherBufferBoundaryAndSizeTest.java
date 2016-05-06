package rsc.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import org.junit.Test;
import rsc.processor.DirectProcessor;
import rsc.test.TestSubscriber;
import rsc.util.ConstructorTestBuilder;

public class PublisherBufferBoundaryAndSizeTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherBufferBoundaryAndSize.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("other", PublisherNever.instance());
        ctb.addInt("maxSize", 1, Integer.MAX_VALUE);
        ctb.addRef("bufferSupplier", (Supplier<List<Object>>)() -> new ArrayList<>());
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test
    public void withMaxSize() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 10).buffer(PublisherNever.instance(), 3).subscribe(ts);
        
        ts.assertValues(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9), Arrays.asList(10))
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void mixed() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        DirectProcessor<Integer> sp1 = new DirectProcessor<>();
        DirectProcessor<Integer> sp2 = new DirectProcessor<>();
        
        sp1.buffer(sp2, 3).subscribe(ts);

        sp1.onNext(1);
        sp1.onNext(2);
        sp2.onNext(100);
        
        sp1.onNext(3);
        sp1.onNext(4);
        sp1.onNext(5);
        sp1.onNext(6);
        sp2.onNext(200);
        
        sp1.onNext(7);
        sp1.onNext(8);
        sp1.onNext(9);
        sp2.onNext(300);
        
        sp1.onNext(10);
        sp2.onComplete();
        
        ts.assertValues(
                Arrays.asList(1, 2), 
                Arrays.asList(3, 4, 5), 
                Arrays.asList(6),
                Arrays.asList(7, 8, 9),
                Arrays.asList(10)
            )
        .assertNoError()
        .assertComplete();
    }
}
