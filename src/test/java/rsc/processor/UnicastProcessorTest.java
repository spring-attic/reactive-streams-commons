package rsc.processor;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import rsc.flow.Fuseable;
import rsc.scheduler.Scheduler;
import rsc.scheduler.SingleScheduler;
import rsc.test.TestSubscriber;
import rsc.util.SpscLinkedArrayQueue;

public class UnicastProcessorTest {

    static Scheduler scheduler;
    
    @BeforeClass
    public static void before() {
        scheduler = new SingleScheduler();
    }
    
    @AfterClass
    public static void after() {
        scheduler.shutdown();
    }
    
    @Test
    public void secondSubscriberRejectedProperly() {
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        up.subscribe();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        up.subscribe(ts);
        
        ts.assertNoValues()
        .assertError(IllegalStateException.class)
        .assertNotComplete();
        
    }
    
    @Test
    public void normalLive() {
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        up.subscribe(ts);

        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onNext(6);
        up.onComplete();
        
        ts.assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void normalOffline() {
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
        
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onNext(6);
        up.onComplete();

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        up.subscribe(ts);

        ts.assertResult(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void subscribeOnNextRace() {
        int n = 100_000;
        
        for (int i = 0; i < n; i++) {
            UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            scheduler.schedule(() -> {
                up.onNext(1);
                up.onNext(2);
                up.onNext(3);
                up.onNext(4);
                up.onNext(5);
                up.onNext(6);
                up.onComplete();
            });
            
            up.subscribe(ts);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertResult(1, 2, 3, 4, 5, 6);
        }
    }
    
    @Test
    public void subscribeOnNextRaceFused() {
        int n = 100_000;
        
        for (int i = 0; i < n; i++) {
            UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            scheduler.schedule(() -> {
                up.onNext(1);
                up.onNext(2);
                up.onNext(3);
                up.onNext(4);
                up.onNext(5);
                up.onNext(6);
                up.onComplete();
            });
            
            up.subscribe(ts);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertFusionMode(Fuseable.ASYNC);
            ts.assertResult(1, 2, 3, 4, 5, 6);
        }
    }
    
    @Test
    public void subscribeOnNextRaceFusedSpin1() {
        int n = 100_000;
        
        for (int i = 0; i < n; i++) {
            UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            AtomicInteger wip = new AtomicInteger(2);
            
            scheduler.schedule(() -> {
                wip.decrementAndGet();
                while (wip.get() != 0) ;

                up.onNext(1);
                up.onNext(2);
                up.onNext(3);
                up.onNext(4);
                up.onNext(5);
                up.onNext(6);
                
                up.onComplete();
            });

            wip.decrementAndGet();
            while (wip.get() != 0) ;

            up.subscribe(ts);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertFusionMode(Fuseable.ASYNC);
            ts.assertResult(1, 2, 3, 4, 5, 6);
        }
    }
    
    @Test
    public void subscribeOnNextRaceFusedSpin2() {
        int n = 100_000;
        
        for (int i = 0; i < n; i++) {
            UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            AtomicInteger wip = new AtomicInteger(2);
            
            scheduler.schedule(() -> {
                up.onNext(1);
                up.onNext(2);
                up.onNext(3);
                
                wip.decrementAndGet();
                while (wip.get() != 0) ;
                
                up.onNext(4);
                up.onNext(5);
                up.onNext(6);
                
                up.onComplete();
            });

            wip.decrementAndGet();
            while (wip.get() != 0) ;

            up.subscribe(ts);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertFusionMode(Fuseable.ASYNC);
            ts.assertResult(1, 2, 3, 4, 5, 6);
        }
    }
    
    @Test
    public void subscribeOnNextRaceFusedSpin3() {
        int n = 100_000;
        
        for (int i = 0; i < n; i++) {
            UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            AtomicInteger wip = new AtomicInteger(2);
            
            scheduler.schedule(() -> {
                up.onNext(1);
                up.onNext(2);
                up.onNext(3);
                up.onNext(4);
                up.onNext(5);
                up.onNext(6);

                wip.decrementAndGet();
                while (wip.get() != 0) ;
                
                up.onComplete();
            });

            wip.decrementAndGet();
            while (wip.get() != 0) ;

            up.subscribe(ts);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertFusionMode(Fuseable.ASYNC);
            ts.assertResult(1, 2, 3, 4, 5, 6);
        }
    }
    
    @Test
    public void subscribeOnNextRaceFusedSpin4() {
        int n = 100_000;
        
        for (int i = 0; i < n; i++) {
            UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscLinkedArrayQueue<>(16));
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            ts.requestedFusionMode(Fuseable.ANY);
            
            AtomicInteger wip = new AtomicInteger(2);
            
            scheduler.schedule(() -> {
                up.onNext(1);
                up.onNext(2);
                up.onNext(3);
                up.onNext(4);
                up.onNext(5);
                up.onNext(6);
                up.onComplete();

                wip.decrementAndGet();
                while (wip.get() != 0) ;
            });

            wip.decrementAndGet();
            while (wip.get() != 0) ;

            up.subscribe(ts);
            
            ts.assertTerminated(5, TimeUnit.SECONDS);
            
            ts.assertFusionMode(Fuseable.ASYNC);
            ts.assertResult(1, 2, 3, 4, 5, 6);
        }
    }
}