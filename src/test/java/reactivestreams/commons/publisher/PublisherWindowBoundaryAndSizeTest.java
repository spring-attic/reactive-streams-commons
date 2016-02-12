package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import org.junit.*;
import org.reactivestreams.Publisher;

import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherWindowBoundaryAndSizeTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherWindowBoundary.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("other", PublisherNever.instance());
        ctb.addRef("processorQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addRef("drainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }

    static <T> TestSubscriber<T> toList(Publisher<T> windows) {
        TestSubscriber<T> ts = new TestSubscriber<>();
        windows.subscribe(ts);
        return ts;
    }

    @SafeVarargs
    static <T> void expect(TestSubscriber<PublisherBase<T>> ts, int index, T... values) {
        toList(ts.values().get(index))
        .assertValues(values)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normal() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.window(sp2, 10).subscribe(ts);
        
        ts.assertValueCount(1);
        
        sp1.onNext(1);
        sp1.onNext(2);
        sp1.onNext(3);
        
        sp2.onNext(1);
        
        sp1.onNext(4);
        sp1.onNext(5);
        
        sp1.onComplete();
        
        ts.assertValueCount(2);

        expect(ts, 0, 1, 2, 3);
        expect(ts, 1, 4, 5);
        
        ts.assertNoError()
        .assertComplete();
        
        Assert.assertFalse("sp1 has subscribers", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers", sp1.hasSubscribers());
    }
    
    @Test
    public void normalOverflow() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.window(sp2, 3).subscribe(ts);

        ts.assertValueCount(1);

        sp1.onNext(1);
        sp1.onNext(2);
        sp1.onNext(3);

        ts.assertValueCount(2);
        
        sp1.onNext(4);
        sp1.onNext(5);
        
        sp2.onNext(100);

        ts.assertValueCount(3);
        
        sp1.onNext(6);
        sp1.onNext(7);
        sp1.onNext(8);

        ts.assertValueCount(4);
        
        sp2.onNext(200);

        ts.assertValueCount(4);
        
        sp1.onComplete();
        
        ts.assertValueCount(4);
        
        expect(ts, 0, 1, 2, 3);
        expect(ts, 1, 4, 5);
        expect(ts, 2, 6, 7, 8);
        expect(ts, 3);

        ts.assertNoError()
        .assertComplete();
        
        Assert.assertFalse("sp1 has subscribers", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers", sp1.hasSubscribers());
    }

    @Test
    public void normalMaxSize() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        
        sp1.window(PublisherNever.instance(), 3).subscribe(ts);

        for (int i = 0; i < 99; i++) {
            sp1.onNext(i);
        }
        sp1.onComplete();
        
        for (int i = 0; i < 33; i++) {
            expect(ts, i, (i * 3), (i * 3 + 1), (i * 3 + 2));
        }
        expect(ts, 33);
        
        ts.assertNoError()
        .assertComplete();
        
        Assert.assertFalse("sp1 has subscribers", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers", sp1.hasSubscribers());
    }

    @Test
    public void normalOtherCompletes() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.window(sp2, 10).subscribe(ts);
        
        ts.assertValueCount(1);
        
        sp1.onNext(1);
        sp1.onNext(2);
        sp1.onNext(3);
        
        sp2.onNext(1);
        
        sp1.onNext(4);
        sp1.onNext(5);
        
        sp2.onComplete();
        
        ts.assertValueCount(2);

        expect(ts, 0, 1, 2, 3);
        expect(ts, 1, 4, 5);
        
        ts.assertNoError()
        .assertComplete();
        
        Assert.assertFalse("sp1 has subscribers", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers", sp1.hasSubscribers());
    }

    @Test
    public void mainError() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.window(sp2, 10).subscribe(ts);
        
        ts.assertValueCount(1);
        
        sp1.onNext(1);
        sp1.onNext(2);
        sp1.onNext(3);
        
        sp2.onNext(1);
        
        sp1.onNext(4);
        sp1.onNext(5);
        
        sp1.onError(new RuntimeException("forced failure"));
        
        ts.assertValueCount(2);

        expect(ts, 0, 1, 2, 3);
        
        toList(ts.values().get(1))
        .assertValues(4, 5)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        ts
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers", sp1.hasSubscribers());
    }

    @Test
    public void otherError() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        
        sp1.window(sp2, 10).subscribe(ts);
        
        ts.assertValueCount(1);
        
        sp1.onNext(1);
        sp1.onNext(2);
        sp1.onNext(3);
        
        sp2.onNext(1);
        
        sp1.onNext(4);
        sp1.onNext(5);
        
        sp2.onError(new RuntimeException("forced failure"));
        
        ts.assertValueCount(2);

        expect(ts, 0, 1, 2, 3);
        
        toList(ts.values().get(1))
        .assertValues(4, 5)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        ts
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
        Assert.assertFalse("sp1 has subscribers", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers", sp1.hasSubscribers());
    }

    @Test(timeout = 60000)
    public void asyncConsumers() {
        for (int maxSize = 1; maxSize < 12; maxSize++) {
            System.out.println("asyncConsumers >> " + maxSize);
            ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
            
            try {
                List<TestSubscriber<Long>> tss = new ArrayList<>();
                
                TestSubscriber<PublisherBase<Long>> ts = new TestSubscriber<PublisherBase<Long>>() {
                    @Override
                    public void onNext(PublisherBase<Long> t) {
                        TestSubscriber<Long> its = new  TestSubscriber<>();
                        tss.add(its);
                        t.observeOn(ForkJoinPool.commonPool()).subscribe(its);
                    }
                };
                
                PublisherBase.interval(1, TimeUnit.MILLISECONDS, exec).take(2500)
                .window(PublisherBase.interval(10, TimeUnit.MILLISECONDS, exec), maxSize)
                .subscribe(ts);
                
                ts.await(5, TimeUnit.SECONDS);
                List<Long> data = new ArrayList<>(2500);
                for (TestSubscriber<Long> its : tss) {
                    its.await(5, TimeUnit.SECONDS);
                    data.addAll(its.values());
                }
                
                for (int i = 0; i < data.size() - 1; i++) {
                    Long a1 = data.get(i);
                    Long a2 = data.get(i + 1);
                    
                    Assert.assertEquals((long)a2, a1 + 1);
                }
            } finally {
                exec.shutdownNow();
            }
        }
    }
}
