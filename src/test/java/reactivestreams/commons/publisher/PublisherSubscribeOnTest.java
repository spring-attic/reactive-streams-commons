package reactivestreams.commons.publisher;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherSubscribeOnTest {
    
    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherSubscribeOn.class);
        
        ctb.addRef("source", PublisherBase.never());
        ctb.addRef("executor", ForkJoinPool.commonPool());
        ctb.addRef("scheduler", (Function<Runnable, Runnable>)r -> r);
    }
    
    @Test
    public void classic() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
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
    public void eagerCancelNoRequest() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool(), true, false).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void eagerCancelNoRequestBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool(), true, false).subscribe(ts);
        
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
    public void nonEagerCancelNoRequest() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool(), false, false).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void nonEagerCancelNoRequestBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool(), false, false).subscribe(ts);
        
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
    public void nonEagerCancelRequestOn() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool(), false, true).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void nonEagerCancelRequestOnBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.range(1, 1000).subscribeOn(ForkJoinPool.commonPool(), false, true).subscribe(ts);
        
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
        
        PublisherBase.just(1).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicJustBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.just(1).subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
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
        
        PublisherBase.<Integer>empty().subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void classicEmptyBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.<Integer>empty().subscribeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void nonEagerCancelJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.just(1).subscribeOn(ForkJoinPool.commonPool(), false, true).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void nonEagerCancelJustBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.just(1).subscribeOn(ForkJoinPool.commonPool(), false, true).subscribe(ts);
        
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
    public void nonEagerEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.<Integer>empty().subscribeOn(ForkJoinPool.commonPool(), false, true).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void nonEagerEmptyBackpressured() throws Exception {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        PublisherBase.<Integer>empty().subscribeOn(ForkJoinPool.commonPool(), false, true).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void callableEvaluatedTheRightTime() {
        
        AtomicInteger count = new AtomicInteger();
        
        PublisherBase<Integer> p = new PublisherCallable<>(() -> count.incrementAndGet()).subscribeOn(ForkJoinPool.commonPool());
        
        Assert.assertEquals(0, count.get());
        
        p.subscribe(new TestSubscriber<>());
        
        Assert.assertEquals(1, count.get());
    }}
