package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherIsEmptyTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherIsEmpty<>(null);
    }
    
    @Test
    public void emptySource() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();
        
        new PublisherIsEmpty<>(PublisherEmpty.instance()).subscribe(ts);
        
        ts.assertValue(true)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void emptySourceBackpressured() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>(0);
        
        new PublisherIsEmpty<>(PublisherEmpty.instance()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(1);
        
        ts.assertValue(true)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void nonEmptySource() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>();
        
        new PublisherIsEmpty<>(new PublisherRange(1, 10)).subscribe(ts);
        
        ts.assertValue(false)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void nonEmptySourceBackpressured() {
        TestSubscriber<Boolean> ts = new TestSubscriber<>(0);
        
        new PublisherIsEmpty<>(new PublisherRange(1, 10)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(1);
        
        ts.assertValue(false)
        .assertComplete()
        .assertNoError();
    }
}
