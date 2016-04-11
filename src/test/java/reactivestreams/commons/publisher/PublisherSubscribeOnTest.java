package reactivestreams.commons.publisher;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;
import reactivestreams.commons.util.ExecutorServiceScheduler;

public class PublisherSubscribeOnTest {
    
    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherSubscribeOn.class);
        
        ctb.addRef("source", Px.never());
        ctb.addRef("executor", ForkJoinPool.commonPool());
        ctb.addRef("scheduler", new ExecutorServiceScheduler(ForkJoinPool.commonPool()));
        
        ctb.test();
    }
    
    @Test
    public void classic() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1000).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 1000).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        Thread.sleep(100);
        
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
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.just(1).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicJustBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.just(1).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
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
    public void classicEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.<Integer>empty().subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicEmptyBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.<Integer>empty().subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void callableEvaluatedTheRightTime() {
        
        AtomicInteger count = new AtomicInteger();
        
        Px<Integer> p = new PublisherCallable<>(() -> count.incrementAndGet()).subscribeOn(ForkJoinPool.commonPool());
        
        Assert.assertEquals(0, count.get());
        
        p.subscribe(new TestSubscriber<>());
        
        Assert.assertEquals(1, count.get());
    }}
