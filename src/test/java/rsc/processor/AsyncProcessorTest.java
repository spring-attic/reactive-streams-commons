package rsc.processor;

import org.junit.Test;

import rsc.flow.Fuseable;
import rsc.test.TestSubscriber;

public class AsyncProcessorTest {

    @Test
    public void online() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        AsyncProcessor<Integer> ap = new AsyncProcessor<>();
        
        ap.subscribe(ts);
        
        ap.onNext(1);
        ap.onNext(2);
        
        ts.assertNoEvents();
        
        ap.onComplete();
        
        ts.assertResult(2);
    }

    @Test
    public void onlineBackpressured() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        AsyncProcessor<Integer> ap = new AsyncProcessor<>();
        
        ap.subscribe(ts);
        
        ap.onNext(1);
        ap.onNext(2);
        
        ts.assertNoEvents();
        
        ap.onComplete();
        
        ts.assertNoEvents();
        
        ts.request(1);
        
        ts.assertResult(2);
    }

    @Test
    public void offline() {
        
        AsyncProcessor<Integer> ap = new AsyncProcessor<>();
        
        ap.onNext(1);
        ap.onNext(2);
        ap.onComplete();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ap.subscribe(ts);
        
        ts.assertResult(2);
    }

    @Test
    public void offlineBackpressured() {

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();
        
        ap.onNext(1);
        ap.onNext(2);
        ap.onComplete();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        ap.subscribe(ts);
        
        ts.assertNoEvents();
        
        ts.request(1);
        
        ts.assertResult(2);
    }
    
    @Test
    public void onlineFuseable() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ANY);
        
        AsyncProcessor<Integer> ap = new AsyncProcessor<>();
        
        ap.subscribe(ts);
        
        ap.onNext(1);
        ap.onNext(2);
        
        ts.assertNoEvents();
        
        ap.onComplete();
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertResult(2);

    }

    @Test
    public void offlineFuseable() {

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();
        
        ap.onNext(1);
        ap.onNext(2);
        ap.onComplete();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ANY);
        
        ap.subscribe(ts);
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertResult(2);

    }

    @Test
    public void offlineFuseableBackpressured() {

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();
        
        ap.onNext(1);
        ap.onNext(2);
        ap.onComplete();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ts.requestedFusionMode(Fuseable.ANY);

        ap.subscribe(ts);

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertNoEvents();

        ts.request(1);
        
        ts.assertResult(2);
    }

    @Test
    public void onlineEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.subscribe(ts);

        ap.onComplete();
        
        ts.assertResult();
    }

    @Test
    public void onlineError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.subscribe(ts);

        ap.onError(new RuntimeException("Forced failure"));
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        ;
    }

    @Test
    public void offlineEmpty() {

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.onComplete();

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ap.subscribe(ts);

        ts.assertResult();
    }

    @Test
    public void offlineError() {

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.onError(new RuntimeException("Forced failure"));

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ap.subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        ;
    }

    @Test
    public void onlineEmptyFused() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ts.requestedFusionMode(Fuseable.ANY);

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.subscribe(ts);

        ap.onComplete();
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertResult();
    }

    @Test
    public void onlineErrorFused() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ts.requestedFusionMode(Fuseable.ANY);

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.subscribe(ts);

        ap.onError(new RuntimeException("Forced failure"));
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        ;
    }

    @Test
    public void offlineEmptyFused() {

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.onComplete();

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ts.requestedFusionMode(Fuseable.ANY);
        
        ap.subscribe(ts);

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertResult();
    }

    @Test
    public void offlineErrorFused() {

        AsyncProcessor<Integer> ap = new AsyncProcessor<>();

        ap.onError(new RuntimeException("Forced failure"));

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        ts.requestedFusionMode(Fuseable.ANY);
        ap.subscribe(ts);

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        ;
    }

}
