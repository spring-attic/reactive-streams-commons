package rsc.publisher;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;

import rsc.processor.*;
import rsc.scheduler.ExecutorServiceScheduler;
import rsc.test.TestSubscriber;
import rsc.util.*;

public class PublisherObserveOnTest {

    static ExecutorService exec;
    
    @BeforeClass
    public static void before() {
        exec = Executors.newSingleThreadExecutor();
    }
    
    @AfterClass
    public static void after() {
        exec.shutdownNow();
    }
    
    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherObserveOn.class);
        
        ctb.addRef("source", Px.never());
        ctb.addRef("executor", exec);
        ctb.addRef("scheduler", new ExecutorServiceScheduler(ForkJoinPool.commonPool()));
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addRef("queueSupplier", Px.defaultQueueSupplier(Integer.MAX_VALUE));
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).hide().observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured1() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 1_000).hide().observeOn(exec).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500);
        
        Thread.sleep(250);
        
        ts.assertValueCount(500)
        .assertNoError()
        .assertNotComplete();

        ts.request(500);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 1_000_000).hide().observeOn(exec).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500_000);
        
        Thread.sleep(250);
        
        ts.assertValueCount(500_000)
        .assertNoError()
        .assertNotComplete();

        ts.request(500_000);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalSyncFused() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalSyncFusedBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 1_000_000).observeOn(exec).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500_000);
        
        Thread.sleep(500);
        
        ts.assertValueCount(500_000)
        .assertNoError()
        .assertNotComplete();

        ts.request(500_000);

        ts.assertTerminated(10, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalAsyncFused() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        for (int i = 0; i < 1_000_000; i++) {
            up.onNext(i);
        }
        up.onComplete();
        
        up.observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalAsyncFusedBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(1024));
        
        for (int i = 0; i < 1_000_000; i++) {
            up.onNext(0);
        }
        up.onComplete();

        up.observeOn(exec).subscribe(ts);
        
        try {
            ts.assertNoValues()
            .assertNoError()
            .assertNotComplete();
            
            ts.request(500_000);
            
            Thread.sleep(250);
            
            ts.assertValueCount(500_000)
            .assertNoError()
            .assertNotComplete();
    
            ts.request(500_000);
    
            if (!ts.await(5, TimeUnit.SECONDS)) {
                ts.cancel();
                Assert.fail("TestSubscriber timed out: " + ts.received());
            }
            
            ts.assertValueCount(1_000_000)
            .assertNoError()
            .assertComplete();
        } finally {
            ts.cancel();
        }
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherError<Integer>(new RuntimeException("forced failure")).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.<Integer>empty().observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void errorDelayed() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherError<Integer> err = new PublisherError<>(new RuntimeException("forced failure"));
        Px.range(1, 1000).concatWith(err).observeOn(exec, true).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void classicJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.just(1).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicJustBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.just(1).observeOn(exec).subscribe(ts);
        
        Thread.sleep(100);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void filtered() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 2_000_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void filtered1() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 2_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalFilteredBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 2_000_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500_000);
        
        Thread.sleep(500);
        
        ts.assertValueCount(500_000)
        .assertNoError()
        .assertNotComplete();

        ts.request(500_000);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalFilteredBackpressured1() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 2_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500);
        
        Thread.sleep(500);
        
        ts.assertValueCount(500)
        .assertNoError()
        .assertNotComplete();

        ts.request(500);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void callableEvaluatedTheRightTime() {
        
        AtomicInteger count = new AtomicInteger();
        
        Px<Integer> p = new PublisherCallable<>(count::incrementAndGet).observeOn(ForkJoinPool.commonPool());
        
        Assert.assertEquals(0, count.get());
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        p.subscribe(ts);
        
        if (!ts.await(5, TimeUnit.SECONDS)) {
            ts.cancel();
            Assert.fail("TestSubscriber timed out");
        }
        
        Assert.assertEquals(1, count.get());
    }

    @Test
    public void prefetchAmountOnlyLoop() {
        for (int i = 0; i < 100000; i++) {
            prefetchAmountOnly();
        }
    }

    @Test
    public void diamondLoop() {
        for(int i = 0; i < 100000; i++){
            diamond();
        }
    }


    public void diamond() {

        DirectProcessor<Integer> sp = new DirectProcessor<>();
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px<Integer> fork1 = sp.map(d -> d).observeOn(exec);
        Px<Integer> fork2 = sp.map(d -> d).observeOn(exec);

        ts.request(256);
        Px.mergeArray(fork1, fork2).observeOn(ForkJoinPool.commonPool()).subscribe(ts);


        new PublisherRange(0, 128).hide().observeOn(ForkJoinPool.commonPool()).subscribe(sp);

        ts.await(5, TimeUnit.SECONDS);
        ts.assertValueCount(256)
          .assertNoError()
          .assertComplete();
    }
    
    @Test
    public void prefetchAmountOnly() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ConcurrentLinkedQueue<Long> clq = new ConcurrentLinkedQueue<>();
        
        Px.range(1, 2).hide()
        .doOnRequest(v -> {
            clq.offer(v);
        })
        .observeOn(exec)
        .subscribe(ts);
        
        ts.await(100, TimeUnit.SECONDS);
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();

        int s = clq.size();
        Assert.assertTrue("More requests?" + clq, s == 1 || s == 2 || s == 3);
        Assert.assertEquals((Long)(long)Px.BUFFER_SIZE, clq.poll());
    }
    
    @Test
    public void boundedQueueLoop() {
        for (int i = 0; i < 1000; i++) {
            boundedQueue();
        }
    }
    
    @Test
    public void boundedQueue() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherObserveOn<>(Px.range(1, 100_000).hide(),
                Px.fromExecutor(exec), true, 128, () -> new SpscArrayQueue<>(128)
        )
        .subscribe(ts);
        
        ts.await(1, TimeUnit.SECONDS);
        
        ts.assertValueCount(100_000)
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void boundedQueueFilterLoop() {
        for (int i = 0; i < 1000; i++) {
            boundedQueueFilter();
        }
    }
    
    @Test
    public void boundedQueueFilter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherObserveOn<>(Px.range(1, 100_000).hide(),
                Px.fromExecutor(exec), true, 128, () -> new SpscArrayQueue<>(128)
        ).filter(v -> (v & 1) == 0)
        .subscribe(ts);
        
        ts.await(1, TimeUnit.SECONDS);
        
        ts.assertValueCount(50_000)
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void withFlatMapLoop() {
        for (int i = 0; i < 200; i++) {
            withFlatMap();
        }
    }

    
    @Test
    public void withFlatMap() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 100_000).flatMap(Px::just).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(100_000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void syncSourceWithNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromArray(1, null, 1).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void syncSourceWithNull2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromIterable(Arrays.asList(1, null, 1)).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromArray(1, 2).map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNullHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromArray(1, 2).hide().map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNullPostFilterHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromArray(1, 2).hide().map(v -> v == 2 ? null : v)
        .observeOn(exec).filter(v -> true).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNull2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromIterable(Arrays.asList(1, 2)).map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNull2Hidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromIterable(Arrays.asList(1, 2)).hide().map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedFilteredSyncSourceWithNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromArray(1, 2).map(v -> v == 2 ? null : v).filter(v -> true).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedFilteredSyncSourceWithNull2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.fromIterable(Arrays.asList(1, 2)).map(v -> v == 2 ? null : v).filter(v -> true).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedAsyncSourceWithNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(2));
        up.onNext(1);
        up.onNext(2);
        up.onComplete();
        
        up.map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedAsyncSourceWithNullPostFilter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(2));
        up.onNext(1);
        up.onNext(2);
        up.onComplete();
        
        up.map(v -> v == 2 ? null : v).observeOn(exec)
        .filter(v -> true).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void crossRangeHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        int count = 1000000;
        
        Px.range(1, count)
        .hide().flatMap(v -> Px.range(v, 2).hide(), false, 1)
        .hide().observeOn(exec).subscribe(ts);
        
        if (!ts.await(5, TimeUnit.SECONDS)) {
            ts.cancel();
        }
        
        ts.assertValueCount(count * 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void crossRange() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        int count = 1000000;
        
        Px.range(1, count)
        .flatMap(v -> Px.range(v, 2), false, 1)
        .observeOn(exec).subscribe(ts);
        
        if (!ts.await(10, TimeUnit.SECONDS)) {
            ts.cancel();
        }
        
        ts.assertValueCount(count * 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void crossRangeMaxHiddenLoop() throws Exception  {
        for (int i = 0; i < 50; i++) {
            crossRangeMaxHidden();
        }
    }

    @Test
    public void crossRangeMaxHidden() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        int count = 1000000;
        
        Px.range(1, count)
        .hide().flatMap(v -> Px.range(v, 2).hide(), false, 32)
        .hide().observeOn(exec).subscribe(ts);
        
        if (!ts.await(10, TimeUnit.SECONDS)) {
            ts.cancel();
            throw new TimeoutException("" + ts.received());
        }
        
        ts.assertValueCount(count * 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void crossRangeMaxLoop() {
        for (int i = 0; i < 50; i++) {
            crossRangeMax();
        }
    }
    
    @Test
    public void crossRangeMax() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        int count = 1000000;
        
        Px.range(1, count)
        .flatMap(v -> Px.range(v, 2), false, 32)
        .observeOn(exec).subscribe(ts);
        
        if (!ts.await(10, TimeUnit.SECONDS)) {
            ts.cancel();
        }
        
        ts.assertValueCount(count * 2)
        .assertNoError()
        .assertComplete();
    }

//    @Test
    public void crossRangeMaxUnboundedLoop() {
        for (int i = 0; i < 50; i++) {
            crossRangeMaxUnbounded();
        }
    }

//    @Test
    public void crossRangeMaxUnbounded() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        int count = 1000000;
        
        Px.range(1, count)
        .flatMap(v -> Px.range(v, 2))
        .observeOn(exec).subscribe(ts);
        
        if (!ts.await(10, TimeUnit.SECONDS)) {
            ts.cancel();
        }
        
        ts.assertValueCount(count * 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void threadBoundaryPreventsInvalidFusionMap() {
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(2));
        
        TestSubscriber<String> ts = new TestSubscriber<>();

        up.map(v -> Thread.currentThread().getName()).observeOn(exec).subscribe(ts);

        up.onNext(1);
        up.onComplete();
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(Thread.currentThread().getName())
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void threadBoundaryPreventsInvalidFusionFilter() {
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(2));
        
        String s = Thread.currentThread().getName();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        up.filter(v -> s.equals(Thread.currentThread().getName())).observeOn(exec).subscribe(ts);

        up.onNext(1);
        up.onComplete();
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void crossRangePerfDefaultLoop() {
        for (int i = 0; i < 100000; i++) {
            if (i % 2000 == 0)
            crossRangePerfDefault();
        }
    }
    
    @Test
    public void crossRangePerfDefault() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        ExecutorServiceScheduler scheduler = new ExecutorServiceScheduler(exec);

        int count = 1000;
        
        Px<Integer> source = Px.range(1, count).flatMap(v -> Px.range(v, 2), false, 32);

        source.observeOn(scheduler).subscribe(ts);

        if (!ts.await(10, TimeUnit.SECONDS)) {
            ts.cancel();
        }
        
        ts.assertValueCount(count * 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void crossRangePerfDefaultLoop2() {
        ExecutorServiceScheduler scheduler = new ExecutorServiceScheduler(exec);

        int count = 1000;

        for (int j = 1; j < 256; j *= 2) {
            
            Px<Integer> source = Px.range(1, count).flatMap(v -> Px.range(v, 2), false, j).observeOn(scheduler);
    
            for (int i = 0; i < 10000; i++) {
                TestSubscriber<Integer> ts = new TestSubscriber<>();
        
                source.subscribe(ts);
        
                if (!ts.await(15, TimeUnit.SECONDS)) {
                    ts.cancel();
                    Assert.fail("Timed out @ maxConcurrency = " + j);
                }
                
                ts.assertValueCount(count * 2)
                .assertNoError()
                .assertComplete();
            }
        }
    }

}
