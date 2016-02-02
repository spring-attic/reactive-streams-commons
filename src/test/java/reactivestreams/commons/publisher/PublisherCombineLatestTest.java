package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.test.TestSubscriber;

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

    @SuppressWarnings("unchecked")
    @Test
    public void intervalResultInCorrectTotalCouples() {
        TestSubscriber<Long[]> ts = new TestSubscriber<>();

        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);

        new PublisherCombineLatest<>(new Publisher[] {
                PublisherBase.interval(50, TimeUnit.MILLISECONDS, exec).take(20),
                PublisherBase.interval(100, TimeUnit.MILLISECONDS, exec).take(20)
        },
        a -> Arrays.asList(a[0] , a[1]), qs, 128)
           // .doOnNext(System.out::println)
            .subscribe(ts);

        ts.await();
        ts.assertValueCount(38)
          .assertComplete()
          .assertNoError();

        exec.shutdown();

    }

    @SuppressWarnings("unchecked")
    @Test
    public void unpairedKeepsRequesting() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        new PublisherCombineLatest<>(new Publisher[] { sp1, sp2 }, a -> (Integer)a[0] + (Integer)a[1], qs, 16).subscribe(ts);
        
        for (int i = 0; i < 17; i++) {
            sp1.onNext(i);
        }
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        sp2.onNext(100);
        
        ts.assertValue(116)
        .assertNoError()
        .assertNotComplete();
    }
}
