package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherRangeTest {

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(1, 10).subscribe(ts);
        
        ts
        .assertNoError()
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRange(1, 10).subscribe(ts);
        
        ts
        .assertNoError()
        .assertNoValues()
        .assertNotComplete();
        
        ts.request(5);
        
        ts
        .assertNoError()
        .assertValues(1, 2, 3, 4, 5)
        .assertNotComplete();
        
        ts.request(10);
        
        ts
        .assertNoError()
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete();
    }

    @Test
    public void normalBackpressuredExact() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(10);
        
        new PublisherRange(1, 10).subscribe(ts);
        
        ts
        .assertNoError()
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete();
        
        ts.request(10);
        
        ts
        .assertNoError()
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete();
    }

    @Test(expected = IllegalArgumentException.class)
    public void countIsNegative() {
        new PublisherRange(1, -1);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void rangeOverflow() {
        new PublisherRange(2, Integer.MAX_VALUE);
    }

    @Test
    public void normalNearMaxValue1() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(Integer.MAX_VALUE, 1).subscribe(ts);
        
        ts
        .assertNoError()
        .assertValue(Integer.MAX_VALUE)
        .assertComplete();
    }

    @Test
    public void normalNearMaxValue2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(Integer.MAX_VALUE - 1, 2).subscribe(ts);
        
        ts
        .assertNoError()
        .assertValues(Integer.MAX_VALUE - 1, Integer.MAX_VALUE)
        .assertComplete();
    }
    
    @Test
    public void normalNegativeStart() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRange(-10, 2).subscribe(ts);
        
        ts
        .assertNoError()
        .assertValues(-10, -9)
        .assertComplete();

    }
}
