package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherIgnoreElementsTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherIgnoreElements<>(null);
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherIgnoreElements<>(new PublisherRange(1, 10)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        new PublisherIgnoreElements<>(new PublisherRange(1, 10)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherIgnoreElements<>(PublisherEmpty.<Integer>instance()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void never() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherIgnoreElements<>(PublisherNever.<Integer>instance()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherIgnoreElements<>(new PublisherError<Integer>(new RuntimeException("Forced failure"))).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        ;
    }
}
