package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherDelaySubscriptionTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherDelaySubscription<>(null, PublisherNever.instance());
    }

    @Test(expected = NullPointerException.class)
    public void otherNull() {
        new PublisherDelaySubscription<>(PublisherNever.instance(), null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherDelaySubscription<>(new PublisherRange(1, 10), new PublisherJust<>(1)).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherDelaySubscription<>(new PublisherRange(1, 10), new PublisherJust<>(1)).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNotComplete()
        .assertNoError();

        ts.request(5);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7)
        .assertNotComplete()
        .assertNoError();

        ts.request(10);
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    
    @Test
    public void manyTriggered() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherDelaySubscription<>(new PublisherRange(1, 10), new PublisherRange(1, 10)).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void manyTriggeredBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherDelaySubscription<>(new PublisherRange(1, 10), new PublisherRange(1, 10)).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNotComplete()
        .assertNoError();

        ts.request(5);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7)
        .assertNotComplete()
        .assertNoError();

        ts.request(10);
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void emptyTrigger() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherDelaySubscription<>(new PublisherRange(1, 10), PublisherEmpty.instance()).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void emptyTriggerBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherDelaySubscription<>(new PublisherRange(1, 10), PublisherEmpty.instance()).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNotComplete()
        .assertNoError();

        ts.request(5);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7)
        .assertNotComplete()
        .assertNoError();

        ts.request(10);
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void neverTriggered() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherDelaySubscription<>(new PublisherRange(1, 10), PublisherNever.instance()).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
    }


}
