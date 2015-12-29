package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherLiftTest {

    @Test(expected = NullPointerException.class)
    public void nullSource() {
        new PublisherLift<>(null, v -> v);
    }
    
    @Test(expected = NullPointerException.class)
    public void lifterNull() {
        new PublisherLift<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void lifterReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherLift<>(new PublisherRange(1, 10), v -> null).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class)
        ;
    }

    @Test
    public void lifterThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherLift<>(new PublisherRange(1, 10), v -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        ;
    }
    
    @Test
    public void passthrough() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherLift<Integer, Integer>(new PublisherRange(1, 10), v -> v).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete()
        .assertNoError()
        ;
    }
}
