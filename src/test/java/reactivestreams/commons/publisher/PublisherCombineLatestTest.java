package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherCombineLatestTest {

    Supplier<Queue<PublisherCombineLatest.SourceAndArray>> qs = ConcurrentLinkedQueue::new;
    
    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherCombineLatest<>((Publisher<Integer>[])null, a -> a, qs, 128);
    }

    @Test(expected = NullPointerException.class)
    public void source2Null() {
        new PublisherCombineLatest<>((Iterable<Publisher<Integer>>)null, a -> a, qs, 128);
    }
    
    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void combiner1Null() {
        new PublisherCombineLatest<>(new Publisher[] { }, null, qs, 128);
    }

    @Test(expected = NullPointerException.class)
    public void combiner2Null() {
        new PublisherCombineLatest<>(Collections.emptyList(), null, qs, 128);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void queueSupplier1Null() {
        new PublisherCombineLatest<>(new Publisher[] { }, a -> a, null, 128);
    }

    @Test(expected = NullPointerException.class)
    public void queueSupplier2Null() {
        new PublisherCombineLatest<>(Collections.emptyList(), a -> a, null, 128);
    }
    @SuppressWarnings("unchecked")

    @Test(expected = IllegalArgumentException.class)
    public void bufferSize1Invalid() {
        new PublisherCombineLatest<>(new Publisher[] { }, a -> a, qs, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bufferSize2Invalid() {
        new PublisherCombineLatest<>(Collections.emptyList(), a -> a, qs, 0);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void normal() {
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        TestSubscriber<List<Object>> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(new Publisher[] { sp1, sp2 }, a -> Arrays.asList(a), qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(1);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(2);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(1);
        
        ts.assertValue(Arrays.asList(2, 1))
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(2);

        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2))
        .assertNoError()
        .assertNotComplete();
        
        sp1.onComplete();
        
        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2))
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(3);
        
        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2), Arrays.asList(2, 3))
        .assertNoError()
        .assertNotComplete();

        sp2.onComplete();

        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2), Arrays.asList(2, 3))
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalIterable() {
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        TestSubscriber<List<Object>> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(Arrays.asList(sp1, sp2), a -> Arrays.asList(a), qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(1);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp1.onNext(2);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(1);
        
        ts.assertValue(Arrays.asList(2, 1))
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(2);

        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2))
        .assertNoError()
        .assertNotComplete();
        
        sp1.onComplete();
        
        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2))
        .assertNoError()
        .assertNotComplete();

        sp2.onNext(3);
        
        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2), Arrays.asList(2, 3))
        .assertNoError()
        .assertNotComplete();

        sp2.onComplete();

        ts.assertValues(Arrays.asList(2, 1), Arrays.asList(2, 2), Arrays.asList(2, 3))
        .assertNoError()
        .assertComplete();
    }

    
    @SuppressWarnings("unchecked")
    @Test
    public void firstEmpty() {
        TestSubscriber<List<Object>> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(new Publisher[] { PublisherEmpty.instance(), PublisherNever.instance() }, a -> Arrays.asList(a), qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
        
    }

    @SuppressWarnings("unchecked")
    @Test
    public void secondEmpty() {
        TestSubscriber<List<Object>> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(new Publisher[] { PublisherNever.instance(), PublisherEmpty.instance() }, a -> Arrays.asList(a), qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
        
    }

    @SuppressWarnings("unchecked")
    @Test
    public void firstError() {
        TestSubscriber<List<Object>> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(new Publisher[] { new PublisherError<>(new RuntimeException("forced failure")), PublisherNever.instance() }, a -> Arrays.asList(a), qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
    }

    @SuppressWarnings("unchecked")
    @Test
    public void secondError() {
        TestSubscriber<List<Object>> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(new Publisher[] { PublisherNever.instance(), new PublisherError<>(new RuntimeException("forced failure")) }, a -> Arrays.asList(a), qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
    }

    @SuppressWarnings("unchecked")
    @Test
    public void combinerThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(new Publisher[] { new PublisherJust<>(1), new PublisherJust<>(2) }, a -> { throw new RuntimeException("forced failure"); }, qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
    }

    @SuppressWarnings("unchecked")
    @Test
    public void combinerReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherCombineLatest<>(new Publisher[] { new PublisherJust<>(1), new PublisherJust<>(2) }, a -> null, qs, 128).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
        
    }
    
}
