package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherSkipUntilTest {
    
    @Test(expected = NullPointerException.class)
    public void nullSource() {
        new PublisherSkipUntil<>(null, PublisherNever.instance());
    }

    @Test(expected = NullPointerException.class)
    public void nullOther() {
        new PublisherSkipUntil<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void skipNone() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), PublisherEmpty.instance()).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5 ,6 ,7 ,8, 9, 10)
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void skipNoneBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), PublisherEmpty.instance()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();
        
        ts.request(10);
        
        ts.assertValues(1, 2, 3, 4, 5 ,6 ,7 ,8, 9, 10)
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void skipAll() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), PublisherNever.instance()).subscribe(ts);
        
        ts.assertNoValues()
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void skipAllBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), PublisherNever.instance()).subscribe(ts);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts.request(2);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void skipNoneOtherMany() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), new PublisherRange(1, 10)).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete()
        .assertNoError()
        ;
    }

    @Test
    public void skipNoneBackpressuredOtherMany() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), new PublisherRange(1, 10)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts.request(10);
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void otherSignalsError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), new PublisherError<>(new RuntimeException("forced failure"))).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        ;
    }

    @Test
    public void otherSignalsErrorBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherSkipUntil<>(new PublisherRange(1, 10), new PublisherError<>(new RuntimeException("forced failure"))).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

}
