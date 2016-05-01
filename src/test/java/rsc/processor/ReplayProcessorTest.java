package rsc.processor;

import org.junit.Assert;
import org.junit.Test;

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

        Assert.assertFalse("Has subscribers?", rp.hasSubscribers());

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

        Assert.assertFalse("Has subscribers?", rp.hasSubscribers());

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
        
        Assert.assertFalse("Has subscribers?", rp.hasSubscribers());
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

        Assert.assertFalse("Has subscribers?", rp.hasSubscribers());

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

        Assert.assertFalse("Has subscribers?", rp.hasSubscribers());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValue(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }

}
