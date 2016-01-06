package reactivestreams.commons.publisher;

import java.util.*;
import java.util.function.Supplier;

import org.junit.*;

import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.subscriber.test.TestSubscriber;
import reactivestreams.commons.support.ConstructorTestBuilder;

public class PublisherBufferBoundaryTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherBufferBoundary.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("other", PublisherNever.instance());
        ctb.addRef("bufferSupplier", (Supplier<List<Integer>>)() -> new ArrayList<>());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.buffer(sp2).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(1);
        sp1.onNext(2);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp2.onNext(1);
        
        ts.assertValue(Arrays.asList(1, 2))
        .assertNoError()
        .assertNotComplete();
        
        sp2.onNext(2);
        
        ts.assertValues(Arrays.asList(1, 2), Collections.emptyList())
        .assertNoError()
        .assertNotComplete();
        
        sp2.onComplete();
        
        ts.assertValues(Arrays.asList(1, 2), Collections.emptyList())
        .assertNoError()
        .assertNotComplete();

        sp1.onNext(3);
        sp1.onNext(4);
        sp1.onComplete();
        
        ts.assertValues(Arrays.asList(1, 2), Collections.emptyList(), Arrays.asList(3, 4))
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void mainError() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.buffer(sp2).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(1);
        sp1.onNext(2);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp2.onNext(1);
        
        ts.assertValue(Arrays.asList(1, 2))
        .assertNoError()
        .assertNotComplete();

        sp1.onError(new RuntimeException("forced failure"));

        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        
        sp2.onNext(2);
        
        ts.assertValues(Arrays.asList(1, 2))
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        sp2.onComplete();
        
        ts.assertValues(Arrays.asList(1, 2))
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void otherError() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.buffer(sp2).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(1);
        sp1.onNext(2);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp2.onNext(1);
        
        ts.assertValue(Arrays.asList(1, 2))
        .assertNoError()
        .assertNotComplete();

        sp1.onNext(3);

        sp2.onError(new RuntimeException("forced failure"));

        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());

        ts.assertValues(Arrays.asList(1, 2))
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        sp2.onComplete();
        
        ts.assertValues(Arrays.asList(1, 2))
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }
    
    @Test
    public void bufferSupplierThrows() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.buffer(sp2, (Supplier<List<Integer>>)() -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void bufferSupplierThrowsLater() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        int count[] = { 1 };
        
        sp1.buffer(sp2, (Supplier<List<Integer>>)() -> {
            if (count[0]-- > 0) {
                return new ArrayList<>();
            }
            throw new RuntimeException("forced failure"); 
        }).subscribe(ts);
        

        sp1.onNext(1);
        sp1.onNext(2);
        
        sp2.onNext(1);

        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void bufferSupplierReturnsNUll() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.buffer(sp2, (Supplier<List<Integer>>)() -> null).subscribe(ts);
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

}
