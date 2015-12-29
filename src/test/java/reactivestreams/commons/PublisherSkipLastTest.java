package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherSkipLastTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherSkipLast<>(null, 1);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void negativeNumber() {
        new PublisherSkipLast<>(PublisherNever.instance(), -1);
    }
    
    @Test
    public void skipNone() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherSkipLast<>(new PublisherRange(1, 10), 0).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void skipNoneBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherSkipLast<>(new PublisherRange(1, 10), 0).subscribe(ts);
        
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
    public void skipSome() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherSkipLast<>(new PublisherRange(1, 10), 3).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void skipSomeBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherSkipLast<>(new PublisherRange(1, 10), 3).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNotComplete()
        .assertNoError();

        ts.request(4);

        ts.assertValues(1, 2, 3, 4, 5, 6)
        .assertNotComplete()
        .assertNoError();

        ts.request(10);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void skipAll() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherSkipLast<>(new PublisherRange(1, 10), 20).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void skipAllBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherSkipLast<>(new PublisherRange(1, 10), 20).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

}
