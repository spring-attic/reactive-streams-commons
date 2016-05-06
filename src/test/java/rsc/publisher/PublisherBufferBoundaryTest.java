package rsc.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import rsc.processor.DirectProcessor;
import rsc.test.TestSubscriber;
import rsc.util.ConstructorTestBuilder;

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
        
        DirectProcessor<Integer> sp1 = new DirectProcessor<>();
        DirectProcessor<Integer> sp2 = new DirectProcessor<>();
        
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


        sp1.onNext(3);
        sp1.onNext(4);

        sp2.onComplete();

        ts.assertValues(Arrays.asList(1, 2), Collections.emptyList(), Arrays.asList(3, 4))
        .assertNoError()
        .assertComplete();

        sp1.onNext(5);
        sp1.onNext(6);
        sp1.onComplete();
        
        ts.assertValues(Arrays.asList(1, 2), Collections.emptyList(), Arrays.asList(3, 4))
        .assertNoError()
          .assertComplete();
    }
    
    @Test
    public void mainError() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        DirectProcessor<Integer> sp1 = new DirectProcessor<>();
        DirectProcessor<Integer> sp2 = new DirectProcessor<>();
        
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

        Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
        
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
        
        DirectProcessor<Integer> sp1 = new DirectProcessor<>();
        DirectProcessor<Integer> sp2 = new DirectProcessor<>();
        
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

        Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

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
        
        DirectProcessor<Integer> sp1 = new DirectProcessor<>();
        DirectProcessor<Integer> sp2 = new DirectProcessor<>();
        
        sp1.buffer(sp2, (Supplier<List<Integer>>)() -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void bufferSupplierThrowsLater() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        DirectProcessor<Integer> sp1 = new DirectProcessor<>();
        DirectProcessor<Integer> sp2 = new DirectProcessor<>();
        
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

        Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void bufferSupplierReturnsNUll() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        DirectProcessor<Integer> sp1 = new DirectProcessor<>();
        DirectProcessor<Integer> sp2 = new DirectProcessor<>();
        
        sp1.buffer(sp2, (Supplier<List<Integer>>)() -> null).subscribe(ts);
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

}
