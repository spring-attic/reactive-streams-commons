package rsc.flow;

import java.util.concurrent.TimeUnit;

import org.junit.*;

import rsc.processor.UnicastProcessor;
import rsc.publisher.Px;
import rsc.scheduler.SingleTimedScheduler;
import rsc.test.TestSubscriber;
import rsc.util.SpscLinkedArrayQueue;

public class FuseableTest {

    @Test
    public void usingWithSyncFuseableSource() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ts.requestedFusionMode(Fuseable.ANY);
        
        Px.using(() -> 1, v -> Px.range(1, 5), v -> { })
        .subscribe(ts);
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void usingWithAsyncFuseableSource() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ts.requestedFusionMode(Fuseable.ANY);
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
        
        Px.using(() -> 1, v -> up, v -> { })
        .subscribe(ts);
        
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onComplete();
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void usingWithSyncFuseableSourceRejecting() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ts.requestedFusionMode(Fuseable.ASYNC | Fuseable.THREAD_BARRIER);
        
        Px.using(() -> 1, v -> Px.range(1, 5).map(u -> u + 1), v -> { })
        .subscribe(ts);
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.NONE)
        .assertValues(2, 3, 4, 5, 6)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void usingWithNonFuseableSource() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ts.requestedFusionMode(Fuseable.ASYNC | Fuseable.THREAD_BARRIER);
        
        Px.using(() -> 1, v -> Px.range(1, 5).hide(), v -> { })
        .subscribe(ts);
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.NONE)
        .assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void rangeHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ts.requestedFusionMode(Fuseable.ANY);
        
        Px.range(1, 5).hide()
        .subscribe(ts);
        
        ts
        .assertNonFuseableSource()
        .assertFusionMode(-1)
        .assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void rangeObserveOnAsyncBack() {
        SingleTimedScheduler s = new SingleTimedScheduler();
        try {
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            Px.range(1, 5).observeOn(s).subscribe(ts);
            
            if (!ts.await(5, TimeUnit.SECONDS)) {
                ts.cancel();
                Assert.fail("TestScheduler timed out. Received: " + ts.received());
            }
            
            ts.assertFuseableSource()
            .assertFusionMode(Fuseable.ASYNC)
            .assertValues(1, 2, 3, 4, 5)
            .assertNoError()
            .assertComplete();
            
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void longrangeObserveOnAsyncBack() {
        SingleTimedScheduler s = new SingleTimedScheduler();
        try {
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            int n = 2_000_000;
            
            Px.range(1, n).observeOn(s).subscribe(ts);
            
            if (!ts.await(5, TimeUnit.SECONDS)) {
                ts.cancel();
                Assert.fail("TestScheduler timed out. Received: " + ts.received());
            }
            
            ts.assertFuseableSource()
            .assertFusionMode(Fuseable.ASYNC)
            .assertValueCount(n)
            .assertNoError()
            .assertComplete();
            
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void longrangeHiddenObserveOnAsyncBack() {
        SingleTimedScheduler s = new SingleTimedScheduler();
        try {
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            int n = 2_000_000;
            
            Px.range(1, n).hide().observeOn(s).subscribe(ts);
            
            if (!ts.await(5, TimeUnit.SECONDS)) {
                ts.cancel();
                Assert.fail("TestScheduler timed out. Received: " + ts.received());
            }
            
            ts.assertFuseableSource()
            .assertFusionMode(Fuseable.ASYNC)
            .assertValueCount(n)
            .assertNoError()
            .assertComplete();
            
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void rangeHiddenObserveOnAsyncBack() {
        SingleTimedScheduler s = new SingleTimedScheduler();
        try {
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            Px.range(1, 5).hide().observeOn(s).subscribe(ts);
            
            if (!ts.await(5, TimeUnit.SECONDS)) {
                ts.cancel();
                Assert.fail("TestScheduler timed out. Received: " + ts.received());
            }
            
            ts.assertFuseableSource()
            .assertFusionMode(Fuseable.ASYNC)
            .assertValues(1, 2, 3, 4, 5)
            .assertNoError()
            .assertComplete();
            
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void unicastObserveOnAsyncBack() {
        SingleTimedScheduler s = new SingleTimedScheduler();
        try {
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            int n = 2_000_000;
            
            UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(65536));
            
            for (int i = 0; i < n; i++) {
                up.onNext(777);
            }
            up.onComplete();
            
            up.observeOn(s).subscribe(ts);
            
            if (!ts.await(5, TimeUnit.SECONDS)) {
                ts.cancel();
                Assert.fail("TestScheduler timed out. Received: " + ts.received());
            }
            
            ts.assertFuseableSource()
            .assertFusionMode(Fuseable.ASYNC)
            .assertValueCount(n)
            .assertNoError()
            .assertComplete();
            
        } finally {
            s.shutdown();
        }
    }

    
    @Test
    public void rangeTakeFuseableConsumer() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ts.requestedFusionMode(Fuseable.ANY);
        
        Px.range(1, 5).take(3)
        .subscribe(ts);
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void unicastTakeFuseableConsumer() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ts.requestedFusionMode(Fuseable.ANY);
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
        
        up.take(3)
        .subscribe(ts);
        
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        
        Assert.assertTrue(up.isCancelled());
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }

}
