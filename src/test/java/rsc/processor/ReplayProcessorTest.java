package rsc.processor;

import org.junit.Assert;
import org.junit.Test;

import rsc.flow.Fuseable;
import rsc.test.TestSubscriber;

public class ReplayProcessorTest {

    @Test
    public void unbounded() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, true);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        rp.subscribe(ts);
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValue(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void bounded() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, false);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        rp.subscribe(ts);
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValue(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void cancel() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, false);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        rp.subscribe(ts);
        
        ts.cancel();
        
        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());
    }

    @Test
    public void unboundedAfter() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, true);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValue(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void boundedAfter() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, false);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValue(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void unboundedLong() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, true);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(Long.MAX_VALUE);
        
        ts.assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void boundedLong() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, false);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(Long.MAX_VALUE);
        
        ts.assertValueCount(16)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedUnboundedAfterLong() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, true);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ASYNC);
        
        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedUnboundedLong() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, true);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ASYNC);

        rp.subscribe(ts);

        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();


        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedBoundedAfterLong() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, false);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ASYNC);
        
        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValueCount(16)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedBoundedLong() {
        ReplayProcessor<Integer> rp = new ReplayProcessor<>(16, false);
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ASYNC);

        rp.subscribe(ts);

        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();


        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

}
