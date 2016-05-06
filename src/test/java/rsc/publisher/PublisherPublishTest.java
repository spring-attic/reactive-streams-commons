package rsc.publisher;

import org.junit.Assert;
import org.junit.Test;

import rsc.flow.Fuseable;
import rsc.processor.DirectProcessor;
import rsc.processor.UnicastProcessor;
import rsc.test.TestSubscriber;
import rsc.util.SpscArrayQueue;

public class PublisherPublishTest {

    @Test
    public void subsequentSum() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 5).publish(o -> Px.zip(a -> (Integer)a[0] + (Integer)a[1], o, o.skip(1))).subscribe(ts);
        
        ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void subsequentSumHidden() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 5).hide().publish(o -> Px.zip(a -> (Integer)a[0] + (Integer)a[1], o, o.skip(1))).subscribe(ts);
        
        ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void subsequentSumAsync() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(16));
        
        up.publish(o -> Px.zip(a -> (Integer)a[0] + (Integer)a[1], o, o.skip(1))).subscribe(ts);
        
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onComplete();
        
        ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void cancelComposes() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        DirectProcessor<Integer> sp = new DirectProcessor<>();
        
        sp.publish(o -> Px.<Integer>never()).subscribe(ts);
        
        Assert.assertTrue("Not subscribed?", sp.hasDownstreams());
        
        ts.cancel();
        
        Assert.assertFalse("Still subscribed?", sp.hasDownstreams());
    }

    @Test
    public void cancelComposes2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        DirectProcessor<Integer> sp = new DirectProcessor<>();
        
        sp.publish(o -> Px.<Integer>empty()).subscribe(ts);
        
        Assert.assertFalse("Still subscribed?", sp.hasDownstreams());
    }

    @Test
    public void innerCanFuse() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Px.never().publish(o -> Px.range(1, 5)).subscribe(ts);
        
        ts.assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1, 2, 3, 4, 5)
        .assertComplete()
        .assertNoError();
    }
}
