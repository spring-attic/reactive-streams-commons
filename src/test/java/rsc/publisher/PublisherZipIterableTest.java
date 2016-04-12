package rsc.publisher;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherZipIterableTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherZipIterable<>(null, Collections.emptyList(), (a, b) -> a);
    }

    @Test(expected = NullPointerException.class)
    public void iterableNull() {
        new PublisherZipIterable<>(PublisherNever.instance(), null, (a, b) -> a);
    }

    @Test(expected = NullPointerException.class)
    public void zipperNull() {
        new PublisherZipIterable<>(PublisherNever.instance(), Collections.emptyList(), null);
    }
    
    @Test
    public void normalSameSize() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 5),
                Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(11, 22, 33, 44, 55)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalSameSizeBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherZipIterable<>(new PublisherRange(1, 5), 
                Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);

        ts.assertValues(11)
        .assertNoError()
        .assertNotComplete();

        ts.request(2);

        ts.assertValues(11, 22, 33)
        .assertNoError()
        .assertNotComplete();

        ts.request(5);

        ts.assertValues(11, 22, 33, 44, 55)
        .assertComplete()
        .assertNoError();
    }
    
    @Test
    public void normalSourceShorter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 4), 
                Arrays.asList(10, 20, 30, 40, 50), (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(11, 22, 33, 44)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalOtherShorter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 5), 
                Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(11, 22, 33, 44)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void sourceEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 0), 
                Arrays.asList(10, 20, 30, 40), (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void otherEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 5), 
                Collections.<Integer>emptyList(), (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void zipperReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 5), 
                Arrays.asList(10, 20, 30, 40, 50), (a, b) -> (Integer)null).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void iterableReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 5), 
                () -> null, (a, b) -> a).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void zipperThrowsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 5), 
                Arrays.asList(10, 20, 30, 40, 50), (a, b) -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void iterableThrowsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherZipIterable<>(new PublisherRange(1, 5), 
                () -> { throw new RuntimeException("forced failure"); }, (a, b) -> a).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }
    
}
