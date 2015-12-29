package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherAccumulateTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherAccumulate<>(null, (a, b) -> a);
    }
    
    @Test(expected = NullPointerException.class)
    public void accumulatorNull() {
        new PublisherAccumulate<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherAccumulate<>(new PublisherRange(1, 10), (a, b) -> b).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherAccumulate<>(new PublisherRange(1, 10), (a, b) -> b).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();
        
        ts.request(8);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void accumulatorThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherAccumulate<>(new PublisherRange(1, 10), (a, b) -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
    
        ts.assertValue(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void accumulatorReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherAccumulate<>(new PublisherRange(1, 10), (a, b) -> (Integer)null).subscribe(ts);
    
        ts.assertValue(1)
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }
}
