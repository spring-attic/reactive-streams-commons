package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherDefaultIfEmptyTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherDefaultIfEmpty<>(null, 1);
    }

    @Test(expected = NullPointerException.class)
    public void valueNull() {
        new PublisherDefaultIfEmpty<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void nonEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherDefaultIfEmpty<>(new PublisherRange(1, 5), 10).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5)
        .assertComplete()
        .assertNoError();
    
    }

    @Test
    public void nonEmptyBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherDefaultIfEmpty<>(new PublisherRange(1, 5), 10).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts.request(10);
        
        ts.assertValues(1, 2, 3, 4, 5)
        .assertComplete()
        .assertNoError();
    
    }

    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherDefaultIfEmpty<>(PublisherEmpty.instance(), 10).subscribe(ts);
        
        ts.assertValue(10)
        .assertComplete()
        .assertNoError();
    
    }

    @Test
    public void emptyBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherDefaultIfEmpty<>(PublisherEmpty.instance(), 10).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertValue(10)
        .assertComplete()
        .assertNoError();
    
    }

}
