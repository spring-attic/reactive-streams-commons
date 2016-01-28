package reactivestreams.commons.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherBufferStartEndTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherBufferStartEnd.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("start", PublisherNever.instance());
        ctb.addRef("end", (Function<Object, Publisher<Object>>)o -> PublisherNever.instance());
        ctb.addRef("bufferSupplier", (Supplier<List<Object>>)() -> new ArrayList<>());
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = SimpleProcessor.create();
        SimpleProcessor<Integer> sp2 = SimpleProcessor.create();
        SimpleProcessor<Integer> sp3 = SimpleProcessor.create();
        SimpleProcessor<Integer> sp4 = SimpleProcessor.create();
        
        sp1.buffer(sp2, v -> v == 1 ? sp3 : sp4).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(1);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(1);
        
        Assert.assertTrue("sp3 has no subscribers?", sp3.hasSubscribers());
        
        sp1.onNext(2);
        sp1.onNext(3);
        sp1.onNext(4);
        
        sp3.onComplete();
        
        ts.assertValue(Arrays.asList(2, 3, 4))
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(5);
        
        sp2.onNext(2);

        Assert.assertTrue("sp4 has no subscribers?", sp4.hasSubscribers());
        
        sp1.onNext(6);
        
        sp4.onComplete();

        ts.assertValues(Arrays.asList(2, 3, 4), Arrays.asList(6))
        .assertNoError()
        .assertNotComplete();
        
        sp1.onComplete();
        
        ts.assertValues(Arrays.asList(2, 3, 4), Arrays.asList(6))
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void startCompletes() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = SimpleProcessor.create();
        SimpleProcessor<Integer> sp2 = SimpleProcessor.create();
        SimpleProcessor<Integer> sp3 = SimpleProcessor.create();
        
        sp1.buffer(sp2, v -> sp3).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(1);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(1);
        sp2.onComplete();
        
        Assert.assertTrue("sp3 has no subscribers?", sp3.hasSubscribers());
        
        sp1.onNext(2);
        sp1.onNext(3);
        sp1.onNext(4);
        
        sp3.onComplete();
        
        ts.assertValue(Arrays.asList(2, 3, 4))
        .assertNoError()
        .assertComplete();

        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp3 has subscribers?", sp3.hasSubscribers());

    }

}
