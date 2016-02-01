package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.processor.UnicastProcessor;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;
import reactivestreams.commons.util.SpscArrayQueue;

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
        
        ctb.addRef("source", PublisherBase.never());
        ctb.addRef("executor", exec);
        ctb.addRef("schedulerFactory", (Callable<? extends Consumer<Runnable>>)() -> r -> { });
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addRef("queueSupplier", PublisherBase.defaultQueueSupplier(Integer.MAX_VALUE));
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1_000_000).hide().observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured1() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 1_000).hide().observeOn(exec).subscribe(ts);
        
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
        
        PublisherBase.range(1, 1_000_000).hide().observeOn(exec).subscribe(ts);
        
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
        
        PublisherBase.range(1, 1_000_000).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalSyncFusedBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 1_000_000).observeOn(exec).subscribe(ts);
        
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
    public void normalAsyncFused() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        for (int i = 0; i < 1_000_000; i++) {
            up.onNext(i);
        }
        up.onComplete();
        
        ((PublisherBase<Integer>)up).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalAsyncFusedBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        for (int i = 0; i < 1_000_000; i++) {
            up.onNext(i);
        }
        up.onComplete();

        ((PublisherBase<Integer>)up).observeOn(exec).subscribe(ts);
        
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
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherError<Integer>(new RuntimeException("Forced failure")).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
    }

    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.<Integer>empty().observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void errorDelayed() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherError<Integer> err = new PublisherError<>(new RuntimeException("Forced failure"));
        PublisherBase.range(1, 1000).concatWith(err).observeOn(exec, true).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
    }

    @Test
    public void classicJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.just(1).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicJustBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.just(1).observeOn(exec).subscribe(ts);
        
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
        
        PublisherBase.range(1, 2_000_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void filtered1() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 2_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalFilteredBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 2_000_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
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
    public void normalFilteredBackpressured1() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 2_000).hide().observeOn(exec).filter(v -> (v & 1) == 0).subscribe(ts);
        
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
    public void callableEvaluatedTheRightTime() {
        
        AtomicInteger count = new AtomicInteger();
        
        PublisherBase<Integer> p = new PublisherCallable<>(count::incrementAndGet).observeOn(ForkJoinPool.commonPool());
        
        Assert.assertEquals(0, count.get());
        
        p.subscribe(new TestSubscriber<>());
        
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

        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        PublisherBase<Integer> fork1 = sp.map(d -> d).observeOn(exec);
        PublisherBase<Integer> fork2 = sp.map(d -> d).observeOn(exec);

        ts.request(256);
        PublisherBase.mergeArray(fork1, fork2).observeOn(ForkJoinPool.commonPool()).subscribe(ts);


        new PublisherRange(0, 128).hide().observeOn(ForkJoinPool.commonPool()).subscribe(sp);

        ts.await();
        ts.assertValueCount(256)
          .assertNoError()
          .assertComplete();
    }
    
    @Test
    public void prefetchAmountOnly() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ConcurrentLinkedQueue<Long> clq = new ConcurrentLinkedQueue<>();
        
        PublisherBase.range(1, 2).hide()
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
        Assert.assertEquals((Long)(long)PublisherBase.BUFFER_SIZE, clq.poll());
    }
    
    @Test
    public void boundedQueueLoop() {
        for (int i = 0; i < 1000; i++) {
//            if (i % 100 == 0) {
//                System.out.println("-- " + i);
//            }
            boundedQueue();
        }
    }
    
    @Test
    public void boundedQueue() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherObserveOn<>(PublisherBase.range(1, 100_000).hide(),
                PublisherBase.fromExecutor(exec), true, 128, () -> new SpscArrayQueue<>(128)
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
//            if (i % 100 == 0) {
//                System.out.println("-- " + i);
//            }
            boundedQueueFilter();
        }
    }
    
    @Test
    public void boundedQueueFilter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherObserveOn<>(PublisherBase.range(1, 100_000).hide(),
                PublisherBase.fromExecutor(exec), true, 128, () -> new SpscArrayQueue<>(128)
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
//            if (i % 100 == 0) {
//                System.out.println("-- " + i);
//            }
            withFlatMap();
        }
    }

    
    @Test
    public void withFlatMap() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 100_000).flatMap(PublisherBase::just).observeOn(exec).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(100_000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void syncSourceWithNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromArray(1, null, 1).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void syncSourceWithNull2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromIterable(Arrays.asList(1, null, 1)).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromArray(1, 2).map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNullHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromArray(1, 2).hide().map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNullPostFilterHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromArray(1, 2).hide().map(v -> v == 2 ? null : v)
        .observeOn(exec).filter(v -> true).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNull2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromIterable(Arrays.asList(1, 2)).map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedsyncSourceWithNull2Hidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromIterable(Arrays.asList(1, 2)).hide().map(v -> v == 2 ? null : v).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedFilteredSyncSourceWithNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromArray(1, 2).map(v -> v == 2 ? null : v).filter(v -> true).observeOn(exec).subscribe(ts);

        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mappedFilteredSyncSourceWithNull2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        PublisherBase.fromIterable(Arrays.asList(1, 2)).map(v -> v == 2 ? null : v).filter(v -> true).observeOn(exec).subscribe(ts);

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

}
