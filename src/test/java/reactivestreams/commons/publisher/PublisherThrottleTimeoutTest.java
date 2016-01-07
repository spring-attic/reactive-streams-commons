package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.*;

import org.junit.*;
import org.reactivestreams.Publisher;

import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.subscriber.test.TestSubscriber;
import reactivestreams.commons.support.ConstructorTestBuilder;

public class PublisherThrottleTimeoutTest {

    @Test
    public void constructor() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherThrottleTimeout.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("throttler", (Function<Object, Publisher<Object>>)o -> PublisherNever.instance());
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        
        sp1.throttleTimeout(v -> v == 1 ? sp2 : sp3).subscribe(ts);
        
        sp1.onNext(1);
        sp2.onNext(1);
        
        ts.assertValue(1)
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(2);
        sp1.onNext(3);
        sp1.onNext(4);
        
        sp3.onNext(2);
        
        ts.assertValues(1, 4)
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(5);
        sp1.onComplete();
        
        ts.assertValues(1, 4, 5)
        .assertNoError()
        .assertComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp3 has subscribers?", sp3.hasSubscribers());
    }
    
    @Test
    public void mainError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.throttleTimeout(v -> sp2).subscribe(ts);
        
        sp1.onNext(1);
        sp1.onError(new RuntimeException("forced failure"));
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
    }
    
    @Test
    public void throttlerError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.throttleTimeout(v -> sp2).subscribe(ts);
        
        sp1.onNext(1);
        sp2.onError(new RuntimeException("forced failure"));
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
    }
    
    @Test
    public void throttlerReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        
        sp1.throttleTimeout(v -> null).subscribe(ts);
        
        sp1.onNext(1);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
    }
}
