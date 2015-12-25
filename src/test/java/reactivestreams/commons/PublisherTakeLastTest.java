package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherTakeLastTest {
    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherTakeLast<>(null, 1);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void negativeNumber() {
        new PublisherTakeLast<>(PublisherNever.instance(), -1);
    }
    
    @Test
    public void takeNone() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 0).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void takeNoneBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 0).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void takeOne() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 1).subscribe(ts);
        
        ts.assertValue(10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void takeOneBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 1).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(2);

        ts.assertValues(10)
        .assertNoError()
        .assertComplete();
    }

    
    @Test
    public void takeSome() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 5).subscribe(ts);
        
        ts.assertValues(6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void takeSomeBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 5).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(2);

        ts.assertValues(6, 7)
        .assertNotComplete()
        .assertNoError();

        ts.request(2);

        ts.assertValues(6, 7, 8, 9)
        .assertNotComplete()
        .assertNoError();

        ts.request(10);
        
        ts.assertValues(6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void takeAll() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 20).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void takeAllBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherTakeLast<>(new PublisherRange(1, 10), 20).subscribe(ts);

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

}
