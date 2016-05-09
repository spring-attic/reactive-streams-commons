package rsc.publisher;

import org.junit.*;

import rsc.flow.Fuseable;
import rsc.test.TestSubscriber;

public class PublisherJustTest {

    @Test(expected = NullPointerException.class)
    public void nullValue() {
        new PublisherJust<Integer>(null);
    }

    @Test
    public void valueSame() {
        Assert.assertSame(1, new PublisherJust<>(1).call());
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.just(1).subscribe(ts);

        ts.assertValue(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        Px.just(1).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValue(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void fused() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Px.just(1).subscribe(ts);
        
        ts.assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertResult(1);
    }
}
