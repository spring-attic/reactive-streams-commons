package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherArrayTest {

    @Test(expected = NullPointerException.class)
    public void arrayNull() {
        new PublisherArray<>((Integer[])null);
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);
        
        ts
        .assertNoError()
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);
        
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
        
        new PublisherArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);
        
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
    
    @Test
    public void arrayContainsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherArray<>(1, 2, 3, 4, 5, null, 7, 8, 9, 10).subscribe(ts);
        
        ts
        .assertError(NullPointerException.class)
        .assertValues(1, 2, 3, 4, 5)
        .assertNotComplete();
    }
}
