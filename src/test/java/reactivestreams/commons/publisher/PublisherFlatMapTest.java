package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;

import org.junit.*;
import org.reactivestreams.Publisher;

import reactivestreams.commons.processor.*;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherFlatMapTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherFlatMap.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("mapper", (Function<Object, Publisher<Object>>)v -> PublisherNever.instance());
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addInt("maxConcurrency", 1, Integer.MAX_VALUE);
        ctb.addRef("mainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addRef("innerQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherRange(v, 2)).subscribe(ts);
        
        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherRange(v, 2)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1000);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertNotComplete();

        ts.request(1000);

        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void mainError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherError<Integer>(new RuntimeException("forced failure"))
        .flatMap(v -> new PublisherJust<>(v)).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void innerError() {
        TestSubscriber<Object> ts = new TestSubscriber<>(0);

        new PublisherJust<>(1).flatMap(v -> new PublisherError<>(new RuntimeException("forced failure"))).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void normalQueueOpt() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherArray<>(v, v + 1)).subscribe(ts);
        
        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalQueueOptBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherArray<>(v, v + 1)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1000);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertNotComplete();

        ts.request(1000);

        ts.assertValueCount(2000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void nullValue() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(v -> new PublisherArray<>((Integer)null)).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void mainEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        Px.<Integer>empty().flatMap(v -> new PublisherJust<>(v)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void innerEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        Px.range(1, 1000).flatMap(v -> Px.<Integer>empty()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flatMapOfJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(Px::just).subscribe(ts);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flatMapOfMixed() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 1000).flatMap(
                v -> v % 2 == 0 ? Px.just(v) : Px.fromIterable(Arrays.asList(v)))
        .subscribe(ts);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flatMapOfMixedBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(v -> v % 2 == 0 ? Px.just(v) : Px.fromIterable(Arrays.asList(v))).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500);

        ts.assertValueCount(500)
        .assertNoError()
        .assertNotComplete();

        ts.request(500);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void flatMapOfMixedBackpressured1() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(v -> v % 2 == 0 ? Px.just(v) : Px.fromIterable(Arrays.asList(v))).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500);

        ts.assertValueCount(500)
        .assertNoError()
        .assertNotComplete();

        ts.request(501);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flatMapOfJustBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(Px::just).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500);

        ts.assertValueCount(500)
        .assertNoError()
        .assertNotComplete();

        ts.request(500);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flatMapOfJustBackpressured1() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 1000).flatMap(Px::just).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(500);

        ts.assertValueCount(500)
        .assertNoError()
        .assertNotComplete();

        ts.request(501);

        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void asyncFusionBefore() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        for (int i = 0; i < 1000; i++) {
            up.onNext(i);
        }
        up.onComplete();
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusionAfter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        for (int i = 0; i < 1000; i++) {
            up.onNext(i);
        }
        up.onComplete();

        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusionConcurrently() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {

            exec.execute(() -> {
                ThreadLocalRandom tlr = ThreadLocalRandom.current();
                
                for (int i = 0; i < 1000; i++) {
                    up.onNext(i);
                    if (tlr.nextInt(10) == 0) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ex) {
                            break;
                        }
                    }
                }
                up.onComplete();
            });
            
            ts.await(1, TimeUnit.SECONDS);
            
            ts.assertValueCount(1000)
            .assertNoError()
            .assertComplete();
        
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    public void asyncFusionConcurrentlyBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {

            exec.execute(() -> {
                ThreadLocalRandom tlr = ThreadLocalRandom.current();
                
                for (int i = 0; i < 1000; i++) {
                    up.onNext(i);
                    if (tlr.nextInt(10) == 0) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ex) {
                            break;
                        }
                    }
                }
                up.onComplete();
            });
            
            ts.assertNoValues()
            .assertNotComplete()
            .assertNoError();

            ts.request(500);

            Thread.sleep(200);
            
            ts.assertValueCount(500)
            .assertNotComplete()
            .assertNoError();

            ts.request(500);

            ts.await(1, TimeUnit.SECONDS);
            
            ts.assertValueCount(1000)
            .assertNoError()
            .assertComplete();
        
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    public void asyncFusionErrorBefore() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        up.onError(new RuntimeException("forced failure"));
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void asyncFusionErrorAfter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();

        up.onError(new RuntimeException("forced failure"));
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void asyncFusionCompleteBefore() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        up.onComplete();
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusionCompleteAfter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        Px.just(1).hide().flatMap(v -> up).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();

        up.onComplete();
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void testMaxConcurrency1() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).flatMap(Px::just, false, 1).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleSubscriberOnly() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        AtomicInteger emission = new AtomicInteger();
        
        Px<Integer> source = Px.range(1, 2).doOnNext(v -> emission.getAndIncrement());
        
        SimpleProcessor<Integer> source1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> source2 = new SimpleProcessor<>();
        
        source.flatMap(v -> v == 1 ? source1 : source2, false, 1).subscribe(ts);
        
        Assert.assertEquals(1, emission.get());
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertFalse("source2 has subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);
        source2.onNext(10);
        
        source1.onComplete();
        
        source2.onNext(2);
        source2.onComplete();
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flatMapUnbounded() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        AtomicInteger emission = new AtomicInteger();
        
        Px<Integer> source = Px.range(1, 1000).doOnNext(v -> emission.getAndIncrement());
        
        SimpleProcessor<Integer> source1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> source2 = new SimpleProcessor<>();
        
        source.flatMap(v -> v == 1 ? source1 : source2).subscribe(ts);
        
        Assert.assertEquals(1000, emission.get());
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        Assert.assertTrue("source1 no subscribers?", source1.hasSubscribers());
        Assert.assertTrue("source2 no  subscribers?", source2.hasSubscribers());
        
        source1.onNext(1);
        source1.onComplete();
        
        source2.onNext(2);
        source2.onComplete();
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void syncFusionIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        
        Px.range(1, 1000).flatMap(v -> Px.fromIterable(list)).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void syncFusionRange() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1000).flatMap(v -> Px.range(v, 1000)).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void syncFusionArray() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Integer[] array = new Integer[1000];
        Arrays.fill(array, 777);
        
        Px.range(1, 1000).flatMap(v -> Px.fromArray(array)).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void innerMapSyncFusion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 1000).flatMap(v -> Px.range(1, 1000).map(w -> w + 1)).subscribe(ts);

        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void innerFilterSyncFusion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 1000).flatMap(v -> Px.range(1, 1000).filter(w -> (w & 1) == 0)).subscribe(ts);

        ts.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void innerSyncMapToNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.just(1).hide().flatMap(v -> Px.range(1, 2).map(w -> w == 2 ? null : w)).subscribe(ts);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }
    
    @Test
    public void innerSyncMapFilterToNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.just(1).hide().flatMap(v -> Px.range(1, 2).map(w -> w == 2 ? null : w).filter(w -> true)).subscribe(ts);
        
        ts.assertValue(1)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }
    
    @Ignore("For debugging purposes")
    @Test
    public void slowDrainLoop() {
        for (int i = 0; i < 50; i++) {
            slowDrain();
        }
    }
    
//    @Ignore("For debugging slowness")
    @Test
    public void slowDrain() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        long t = System.nanoTime();
        
        Px.range(1, 100_000).flatMap(v -> Px.range(1, 10)).subscribe(ts);
        
        long t1 = System.nanoTime();
        System.out.printf("%,d%n", t1 - t);
        
        while (ts.values().size() < 1_000_000) {
            ts.request(96);
        }
        
        long t2 = System.nanoTime();
        System.out.printf("%,d%n", t2 - t1);
        
        ts.assertValueCount(1_000_000);
        ts.assertComplete();
        ts.assertNoError();
    }
}
