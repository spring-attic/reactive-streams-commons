package reactivestreams.commons;

import java.util.Arrays;

import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherFilterTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherFilter<Integer>(null, e -> true);
    }
    
    @Test(expected = NullPointerException.class)
    public void predicateNull() {
        new PublisherFilter<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherFilter<>(new PublisherRange(1, 10), v -> v % 2 == 0).subscribe(ts);
        
        ts.assertValues(2, 4, 6, 8, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalBackpressuredRange() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);
        
        new PublisherFilter<>(new PublisherRange(1, 10), v -> v % 2 == 0).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNotComplete()
        .assertNoError();
        
        ts.request(10);
        
        ts.assertValues(2, 4, 6, 8, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalBackpressuredArray() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);
        
        new PublisherFilter<>(new PublisherArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), v -> v % 2 == 0).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNotComplete()
        .assertNoError();
        
        ts.request(10);
        
        ts.assertValues(2, 4, 6, 8, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalBackpressuredIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);
        
        new PublisherFilter<>(new PublisherIterable<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), v -> v % 2 == 0).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNotComplete()
        .assertNoError();
        
        ts.request(10);
        
        ts.assertValues(2, 4, 6, 8, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void predicateThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);
        
        new PublisherFilter<>(new PublisherRange(1, 10), v -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }
}
