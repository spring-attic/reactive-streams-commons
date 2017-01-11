package rsc.publisher;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;

import rsc.flow.Fuseable;
import rsc.scheduler.*;
import rsc.test.TestSubscriber;
import rsc.util.ConstructorTestBuilder;

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
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        p.subscribe(ts);
        
        if (!ts.await(5, TimeUnit.SECONDS)) {
            ts.cancel();
            Assert.fail("TestSubscriber timed out");
        }
        
        Assert.assertEquals(1, count.get());
    }

    @Test
    public void callableEmitsOnScheduler() {
        Scheduler s = new SingleTimedScheduler("pxsingle-");
        
        try {
            TestSubscriber<String> ts = new TestSubscriber<>();
            
            String main = Thread.currentThread().getName();
            
            Px.fromCallable(() -> Thread.currentThread().getName()).subscribeOn(s).subscribe(ts);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertValueCount(1)
            .assertNoError();
            
            Assert.assertNotSame(main, ts.values().get(0));
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void callableLateRequestEmitsOnScheduler() throws InterruptedException {
        Scheduler s = new SingleTimedScheduler("pxsingle-");
        
        try {
            TestSubscriber<String> ts = new TestSubscriber<>(0);
            
            String main = Thread.currentThread().getName();
            
            Px.fromCallable(() -> Thread.currentThread().getName()).subscribeOn(s).subscribe(ts);
            
            Thread.sleep(500);
            
            ts.request(1);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertValueCount(1)
            .assertNoError();
            
            Assert.assertNotSame(main, ts.values().get(0));
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void callableAsyncFuseable() {
        Scheduler s = new SingleTimedScheduler("pxsingle-");
        
        try {
            TestSubscriber<String> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            String main = Thread.currentThread().getName();
            
            Px.fromCallable(() -> Thread.currentThread().getName()).subscribeOn(s).subscribe(ts);
            
            ts
            .assertFusionMode(Fuseable.ASYNC)
            .assertTerminated(5, TimeUnit.SECONDS)
            .assertValueCount(1)
            .assertNoError();
            
            Assert.assertNotSame(main, ts.values().get(0));
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void scalarAsyncFuseable() {
        Scheduler s = new SingleTimedScheduler("pxsingle-");
        
        try {
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            Px.just(1).subscribeOn(s).subscribe(ts);
            
            ts
            .assertFusionMode(Fuseable.ASYNC)
            .assertTerminated(5, TimeUnit.SECONDS)
            .assertValue(1)
            .assertNoError();
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void emptyAsyncFuseable() {
        Scheduler s = new SingleTimedScheduler("pxsingle-");
        
        try {
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            Px.<Integer>empty().subscribeOn(s).subscribe(ts);
            
            ts
            .assertFusionMode(Fuseable.ASYNC)
            .assertTerminated(5, TimeUnit.SECONDS)
            .assertNoValues()
            .assertNoError();
        } finally {
            s.shutdown();
        }
    }

}
