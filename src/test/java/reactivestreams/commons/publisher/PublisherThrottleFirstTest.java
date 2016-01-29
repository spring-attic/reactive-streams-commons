package reactivestreams.commons.publisher;

import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherThrottleFirstTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherThrottleFirst.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("throttler", (Function<Object, Publisher<Object>>)v -> PublisherNever.instance());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        
        sp1.throttleFirst(v -> v == 1 ? sp2 : sp3).subscribe(ts);
        
        sp1.onNext(1);
        
        ts.assertValue(1)
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(2);
        
        ts.assertValue(1)
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(1);
        
        ts.assertValue(1)
        .assertNoError()
        .assertNotComplete();

        sp1.onNext(3);
        
        ts.assertValues(1, 3)
        .assertNoError()
        .assertNotComplete();
        
        sp1.onComplete();
        
        ts.assertValues(1, 3)
        .assertNoError()
        .assertComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp1 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp1 has subscribers?", sp3.hasSubscribers());
    }
    
    @Test
    public void mainError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        
        sp1.throttleFirst(v -> v == 1 ? sp2 : sp3).subscribe(ts);
        
        sp1.onNext(1);
        sp1.onError(new RuntimeException("forced failure"));
        
        ts.assertValue(1)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp1 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp1 has subscribers?", sp3.hasSubscribers());
    }

    @Test
    public void throttlerError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        
        sp1.throttleFirst(v -> v == 1 ? sp2 : sp3).subscribe(ts);
        
        sp1.onNext(1);
        sp2.onError(new RuntimeException("forced failure"));
        
        ts.assertValue(1)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp1 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp1 has subscribers?", sp3.hasSubscribers());
    }

    @Test
    public void throttlerThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        
        sp1.throttleFirst(v -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        sp1.onNext(1);
        
        ts.assertValue(1)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
    }

    @Test
    public void throttlerReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        
        sp1.throttleFirst(v -> null).subscribe(ts);
        
        sp1.onNext(1);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
    }

}
