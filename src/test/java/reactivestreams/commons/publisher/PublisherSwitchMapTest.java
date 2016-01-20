package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.subscriber.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherSwitchMapTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherSwitchMap.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("mapper", (Function<Object, Object>)v -> v);
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)ConcurrentLinkedQueue::new);
        ctb.addInt("bufferSize", 1, Integer.MAX_VALUE);
        
        ctb.test();
    }
    
    @Test
    public void noswitch() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> sp2).subscribe(ts);
        
        sp1.onNext(1);
        
        sp2.onNext(10);
        sp2.onNext(20);
        sp2.onNext(30);
        sp2.onNext(40);
        sp2.onComplete();
        
        ts.assertValues(10, 20, 30, 40)
        .assertNoError()
        .assertNotComplete();
        
        sp1.onComplete();
        
        ts.assertValues(10, 20, 30, 40)
        .assertNoError()
        .assertComplete();
        
    }

    @Test
    public void noswitchBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> sp2).subscribe(ts);
        
        sp1.onNext(1);
        
        sp2.onNext(10);
        sp2.onNext(20);
        sp2.onNext(30);
        sp2.onNext(40);
        sp2.onComplete();
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(10, 20)
        .assertNoError()
        .assertNotComplete();
        
        sp1.onComplete();
        
        ts.assertValues(10, 20)
        .assertNoError()
        .assertNotComplete();

        ts.request(2);

        ts.assertValues(10, 20, 30, 40)
        .assertNoError()
        .assertComplete();
        
    }

    @Test
    public void doswitch() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> v == 1 ? sp2 : sp3).subscribe(ts);
        
        sp1.onNext(1);
        
        sp2.onNext(10);
        sp2.onNext(20);
        
        sp1.onNext(2);
        
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        
        sp2.onNext(30);
        sp3.onNext(300);
        sp2.onNext(40);
        sp3.onNext(400);
        sp2.onComplete();
        sp3.onComplete();
        
        ts.assertValues(10, 20, 300, 400)
        .assertNoError()
        .assertNotComplete();
        
        sp1.onComplete();
        
        ts.assertValues(10, 20, 300, 400)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void mainCompletesBefore() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> sp2).subscribe(ts);
        
        sp1.onNext(1);
        sp1.onComplete();
        
        ts
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp2.onNext(10);
        sp2.onNext(20);
        sp2.onNext(30);
        sp2.onNext(40);
        sp2.onComplete();
        
        ts.assertValues(10, 20, 30, 40)
        .assertNoError()
        .assertComplete();
        
    }

    @Test
    public void mainError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> sp2).subscribe(ts);
        
        sp1.onNext(1);
        sp1.onError(new RuntimeException("forced failure"));
        
        sp2.onNext(10);
        sp2.onNext(20);
        sp2.onNext(30);
        sp2.onNext(40);
        sp2.onComplete();
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void innerError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> sp2).subscribe(ts);
        
        sp1.onNext(1);
        
        sp2.onNext(10);
        sp2.onNext(20);
        sp2.onNext(30);
        sp2.onNext(40);
        sp2.onError(new RuntimeException("forced failure"));
        
        ts.assertValues(10, 20, 30, 40)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
    }

    @Test
    public void mapperThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        sp1.onNext(1);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void mapperReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        
        sp1.switchMap(v -> null).subscribe(ts);
        
        sp1.onNext(1);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }
}
