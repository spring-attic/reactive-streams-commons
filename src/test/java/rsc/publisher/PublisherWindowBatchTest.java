package rsc.publisher;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import org.junit.*;
import org.reactivestreams.Publisher;

import rsc.processor.SimpleProcessor;
import rsc.test.TestSubscriber;
import rsc.util.*;
import rsc.scheduler.SingleTimedScheduler;

public class PublisherWindowBatchTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherWindowBatch.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("boundarySupplier", (Supplier<Publisher<Object>>)() -> Px.never());
        ctb.addRef("mainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addRef("windowQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addInt("maxSize", 1, Integer.MAX_VALUE);
        
        ctb.test();
    }
    
    @Test(timeout = 2000)
    public void normal() {
        SimpleProcessor<Integer> main = new SimpleProcessor<>();

        SimpleProcessor<Integer> b1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> b2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> b3 = new SimpleProcessor<>();
        
        int[] calls = { 0 };

        TestSubscriber<Px<Integer>> ts = new TestSubscriber<>();
        
        main.windowBatch(3, () -> {
            int c = calls[0]++;
            if (c == 0) {
                return b1;
            } else
            if (c == 1) {
                return b2;
            } else
            if (c == 2) {
                return b3;
            }
            return Px.never();
        }).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        Assert.assertTrue("b1 has no subscribers?", b1.hasSubscribers());
        
        main.onNext(1);
        
        ts.assertValueCount(1)
        .assertNoError()
        .assertNotComplete();
        
        main.onNext(2);
        main.onNext(3);
        
        Assert.assertFalse("b1 has subscribers?", b1.hasSubscribers());
        Assert.assertTrue("b2 has no subscribers?", b2.hasSubscribers());
        
        main.onNext(4);

        ts.assertValueCount(2)
        .assertNoError()
        .assertNotComplete();

        b2.onNext(100);

        Assert.assertFalse("b2 has subscribers?", b2.hasSubscribers());
        Assert.assertTrue("b3 has no subscribers?", b3.hasSubscribers());
        
        b3.onComplete();

        Assert.assertFalse("b2 has subscribers?", b3.hasSubscribers());
        
        main.onComplete();
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        expect(ts, 0, 1, 2, 3);
        expect(ts, 1, 4);
    }

    @Test(timeout = 2000)
    public void normalSameLast() {
        SimpleProcessor<Integer> main = new SimpleProcessor<>();

        SimpleProcessor<Integer> b1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> b2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> b3 = new SimpleProcessor<>();
        
        int[] calls = { 0 };

        TestSubscriber<Px<Integer>> ts = new TestSubscriber<>();
        
        main.windowBatch(3, () -> {
            int c = calls[0]++;
            if (c == 0) {
                return b1;
            } else
            if (c == 1) {
                return b2;
            }
            return b3;
        }).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        Assert.assertTrue("b1 has no subscribers?", b1.hasSubscribers());
        
        main.onNext(1);
        
        ts.assertValueCount(1)
        .assertNoError()
        .assertNotComplete();
        
        main.onNext(2);
        main.onNext(3);
        
        Assert.assertFalse("b1 has subscribers?", b1.hasSubscribers());
        Assert.assertTrue("b2 has no subscribers?", b2.hasSubscribers());
        
        main.onNext(4);

        ts.assertValueCount(2)
        .assertNoError()
        .assertNotComplete();

        b2.onNext(100);

        Assert.assertFalse("b2 has subscribers?", b2.hasSubscribers());
        Assert.assertTrue("b3 has no subscribers?", b3.hasSubscribers());
        
        b3.onComplete();

        Assert.assertFalse("b2 has subscribers?", b3.hasSubscribers());
        
        main.onComplete();
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        expect(ts, 0, 1, 2, 3);
        expect(ts, 1, 4);
    }

    @Test(timeout = 10000)
    public void windowBatching() {
        SingleTimedScheduler exec = new SingleTimedScheduler();
        
        try {
            TestSubscriber<Px<Long>> ts = new TestSubscriber<>();
            
            PublisherRange.interval(1, TimeUnit.MILLISECONDS, exec).take(2000)
                          .windowBatch(100, () -> Px.timer(99, TimeUnit.MILLISECONDS, exec))
                          .subscribe(ts);
            
            if (!ts.await(5, TimeUnit.SECONDS)) {
                ts.cancel();
                Assert.fail("Source didn't complete in time");
            }
            
            List<Long> values = new ArrayList<>(2000);
            for (Px<Long> w : ts.values()) {
                values.addAll(w.toList().blockingFirst());
            }
            
            Assert.assertEquals(2000, values.size());
            
            for (int i = 0; i < values.size() - 1; i++) {
                long v1 = values.get(i);
                long v2 = values.get(i + 1);

                Assert.assertEquals("Discontinuity @ " + i + " v1=" + v1 + ", v2=" + v2, v1 + 1, v2);
            }
            
        } finally {
            exec.shutdown();
        }
    }
    
    static <T> TestSubscriber<T> toList(Publisher<T> windows) {
        TestSubscriber<T> ts = new TestSubscriber<>();
        windows.subscribe(ts);
        return ts;
    }

    @SafeVarargs
    static <T> void expect(TestSubscriber<Px<T>> ts, int index, T... values) {
        toList(ts.values().get(index))
        .assertValues(values)
        .assertComplete()
        .assertNoError();
    }

}
