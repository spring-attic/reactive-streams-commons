package reactivestreams.commons.publisher;

import java.util.concurrent.*;
import java.util.function.Function;

import org.junit.*;

import reactivestreams.commons.processor.UnicastProcessor;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

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
        ctb.addRef("scheduler", (Function<Runnable, Runnable>)r -> r);
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addRef("queueSupplier", PublisherBase.defaultQueueSupplier());
        
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

}
