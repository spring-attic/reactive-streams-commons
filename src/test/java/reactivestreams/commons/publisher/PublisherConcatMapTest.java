package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.publisher.PublisherConcatMap.ErrorMode;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherConcatMapTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherConcatMap.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("mapper", (Function<Object, Publisher<Object>>)v -> PublisherNever.instance());
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addRef("errorMode", PublisherConcatMap.ErrorMode.IMMEDIATE);
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 2).concatMap(v -> PublisherBase.range(v, 2)).subscribe(ts);
        
        ts.assertValues(1, 2, 2, 3)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normal2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 2).hide().concatMap(v -> PublisherBase.range(v, 2)).subscribe(ts);
        
        ts.assertValues(1, 2, 2, 3)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBoundary() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 2).concatMap(v -> PublisherBase.range(v, 2), PublisherConcatMap.ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertValues(1, 2, 2, 3)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBoundary2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 2).hide().concatMap(v -> PublisherBase.range(v, 2), PublisherConcatMap.ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertValues(1, 2, 2, 3)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalLongRun() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).concatMap(v -> PublisherBase.range(v, 1000)).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalLongRunJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000_000).concatMap(v -> PublisherBase.just(v)).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalLongRun2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).hide().concatMap(v -> PublisherBase.range(v, 1000)).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalLongRunBoundary() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).concatMap(v -> PublisherBase.range(v, 1000), ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalLongRunJustBoundary() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000_000).concatMap(v -> PublisherBase.just(v), ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    
    @Test
    public void normalLongRunBoundary2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).hide().concatMap(v -> PublisherBase.range(v, 1000), ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleSubscriberOnly() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> source = SimpleProcessor.create();
        
        SimpleProcessor<Integer> source1 = SimpleProcessor.create();
        SimpleProcessor<Integer> source2 = SimpleProcessor.create();
        
        source.concatMap(v -> v == 1 ? source1 : source2).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        source.onNext(1);
        source.onNext(2);
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);
        source2.onNext(10);
        
        source1.onComplete();
        source.onComplete();
        
        source2.onNext(2);
        source2.onComplete();
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleSubscriberOnlyBoundary() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> source = SimpleProcessor.create();
        
        SimpleProcessor<Integer> source1 = SimpleProcessor.create();
        SimpleProcessor<Integer> source2 = SimpleProcessor.create();
        
        source.concatMap(v -> v == 1 ? source1 : source2, ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        source.onNext(1);
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);
        source2.onNext(10);
        
        source1.onComplete();
        source.onNext(2);
        source.onComplete();
        
        source2.onNext(2);
        source2.onComplete();
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();

        Assert.assertFalse("source1 has subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
    }

    @Test
    public void mainErrorsImmediate() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> source = SimpleProcessor.create();
        
        SimpleProcessor<Integer> source1 = SimpleProcessor.create();
        SimpleProcessor<Integer> source2 = SimpleProcessor.create();
        
        source.concatMap(v -> v == 1 ? source1 : source2).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        source.onNext(1);
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);

        source.onError(new RuntimeException("Forced failure"));
        
        ts.assertValue(1)
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("source1 has subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
    }

    @Test
    public void mainErrorsBoundary() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> source = SimpleProcessor.create();
        
        SimpleProcessor<Integer> source1 = SimpleProcessor.create();
        SimpleProcessor<Integer> source2 = SimpleProcessor.create();
        
        source.concatMap(v -> v == 1 ? source1 : source2, ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        source.onNext(1);
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);

        source.onError(new RuntimeException("Forced failure"));
        
        ts.assertValues(1)
        .assertNoError()
        .assertNotComplete();

        source1.onNext(2);
        source1.onComplete();

        ts.assertValues(1, 2)
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("source1 has subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
    }

    @Test
    public void innerErrorsImmediate() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> source = SimpleProcessor.create();
        
        SimpleProcessor<Integer> source1 = SimpleProcessor.create();
        SimpleProcessor<Integer> source2 = SimpleProcessor.create();
        
        source.concatMap(v -> v == 1 ? source1 : source2).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        source.onNext(1);
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);

        source1.onError(new RuntimeException("Forced failure"));
        
        ts.assertValues(1)
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("source1 has subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
    }

    @Test
    public void innerErrorsBoundary() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> source = SimpleProcessor.create();
        
        SimpleProcessor<Integer> source1 = SimpleProcessor.create();
        SimpleProcessor<Integer> source2 = SimpleProcessor.create();
        
        source.concatMap(v -> v == 1 ? source1 : source2, ErrorMode.BOUNDARY).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        source.onNext(1);
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);

        source1.onError(new RuntimeException("Forced failure"));
        
        ts.assertValues(1)
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("source1 has subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
    }

    @Test
    public void innerErrorsEnd() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> source = SimpleProcessor.create();
        
        SimpleProcessor<Integer> source1 = SimpleProcessor.create();
        SimpleProcessor<Integer> source2 = SimpleProcessor.create();
        
        source.concatMap(v -> v == 1 ? source1 : source2, ErrorMode.END).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        source.onNext(1);
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);

        source1.onError(new RuntimeException("Forced failure"));
        
        source.onNext(2);

        Assert.assertTrue("source2 no subscribers?", source2.hasSubscribers());

        source2.onNext(2);
        source2.onComplete();
        
        source.onComplete();
        
        ts.assertValues(1, 2)
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("source1 has subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
    }

    
}
