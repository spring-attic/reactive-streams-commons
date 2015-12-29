package reactivestreams.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherBufferTest {
    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherBuffer<>(null, 1, ArrayList::new);
    }

    @Test(expected = NullPointerException.class)
    public void supplierNull() {
        new PublisherBuffer<>(PublisherNever.instance(), 1, 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sizeZero() {
        new PublisherBuffer<>(PublisherNever.instance(), 0, 1, ArrayList::new);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void skipZero() {
        new PublisherBuffer<>(PublisherNever.instance(), 1, 0, ArrayList::new);
    }

    
    @Test
    public void normalExact() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
    
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, ArrayList::new).subscribe(ts);
    
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Arrays.asList(5, 6),
                Arrays.asList(7, 8),
                Arrays.asList(9, 10)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void normalExactBackpressured() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>(0);
    
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, ArrayList::new).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        )
        .assertNoError()
        .assertNotComplete();
        
        ts.request(3);
        
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Arrays.asList(5, 6),
                Arrays.asList(7, 8),
                Arrays.asList(9, 10)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void largerSkip() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
    
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, 3, ArrayList::new).subscribe(ts);
    
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(4, 5),
                Arrays.asList(7, 8),
                Arrays.asList(10)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void largerSkipEven() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
    
        new PublisherBuffer<>(new PublisherRange(1, 8), 2, 3, ArrayList::new).subscribe(ts);
    
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(4, 5),
                Arrays.asList(7, 8)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void largerSkipEvenBackpressured() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>(0);
    
        new PublisherBuffer<>(new PublisherRange(1, 8), 2, 3, ArrayList::new).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(4, 5)
        )
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(4, 5),
                Arrays.asList(7, 8)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void largerSkipBackpressured() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>(0);
    
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, 3, ArrayList::new).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(4, 5)
        )
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(4, 5),
                Arrays.asList(7, 8),
                Arrays.asList(10)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void smallerSkip() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
    
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, 1, ArrayList::new).subscribe(ts);
    
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(2, 3),
                Arrays.asList(3, 4),
                Arrays.asList(4, 5),
                Arrays.asList(5, 6),
                Arrays.asList(6, 7),
                Arrays.asList(7, 8),
                Arrays.asList(8, 9),
                Arrays.asList(9, 10),
                Arrays.asList(10)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void smallerSkipBackpressured() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>(0);
    
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, 1, ArrayList::new).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(2, 3)
        )
        .assertNoError()
        .assertNotComplete();

        ts.request(2);

        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(2, 3),
                Arrays.asList(3, 4),
                Arrays.asList(4, 5)
        )
        .assertNoError()
        .assertNotComplete();

        ts.request(5);

        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(2, 3),
                Arrays.asList(3, 4),
                Arrays.asList(4, 5),
                Arrays.asList(5, 6),
                Arrays.asList(6, 7),
                Arrays.asList(7, 8),
                Arrays.asList(8, 9),
                Arrays.asList(9, 10)
        )
        .assertNoError()
        .assertNotComplete();

        ts.request(1);
        
        ts.assertValues(
                Arrays.asList(1, 2),
                Arrays.asList(2, 3),
                Arrays.asList(3, 4),
                Arrays.asList(4, 5),
                Arrays.asList(5, 6),
                Arrays.asList(6, 7),
                Arrays.asList(7, 8),
                Arrays.asList(8, 9),
                Arrays.asList(9, 10),
                Arrays.asList(10)
        )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void smallerSkip3Backpressured() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>(0);
    
        new PublisherBuffer<>(new PublisherRange(1, 10), 3, 1, ArrayList::new).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertValues(
                Arrays.asList(1, 2, 3),
                Arrays.asList(2, 3, 4)
        )
        .assertNoError()
        .assertNotComplete();

        ts.request(2);

        ts.assertValues(
                Arrays.asList(1, 2, 3),
                Arrays.asList(2, 3, 4),
                Arrays.asList(3, 4, 5),
                Arrays.asList(4, 5, 6)
        )
        .assertNoError()
        .assertNotComplete();

        ts.request(4);

        ts.assertValues(
                Arrays.asList(1, 2, 3),
                Arrays.asList(2, 3, 4),
                Arrays.asList(3, 4, 5),
                Arrays.asList(4, 5, 6),
                Arrays.asList(5, 6, 7),
                Arrays.asList(6, 7, 8),
                Arrays.asList(7, 8, 9),
                Arrays.asList(8, 9, 10)
        )
        .assertNoError()
        .assertNotComplete();

        ts.request(1);

        ts.assertValues(
                Arrays.asList(1, 2, 3),
                Arrays.asList(2, 3, 4),
                Arrays.asList(3, 4, 5),
                Arrays.asList(4, 5, 6),
                Arrays.asList(5, 6, 7),
                Arrays.asList(6, 7, 8),
                Arrays.asList(7, 8, 9),
                Arrays.asList(8, 9, 10),
                Arrays.asList(9, 10)
        )
        .assertNoError()
        .assertNotComplete();

        ts.request(1);
        
        ts.assertValues(
                Arrays.asList(1, 2, 3),
                Arrays.asList(2, 3, 4),
                Arrays.asList(3, 4, 5),
                Arrays.asList(4, 5, 6),
                Arrays.asList(5, 6, 7),
                Arrays.asList(6, 7, 8),
                Arrays.asList(7, 8, 9),
                Arrays.asList(8, 9, 10),
                Arrays.asList(9, 10),
                Arrays.asList(10)
         )
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void supplierReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, 1, () -> null).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void supplierThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherBuffer<>(new PublisherRange(1, 10), 2, 1, () -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

}
