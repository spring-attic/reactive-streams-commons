package reactivestreams.commons;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherRetryWhenTest {

    Publisher<Integer> justError = new PublisherConcatArray<>(new PublisherJust<>(1), new PublisherError<>(new RuntimeException("forced failure 0")));

    Publisher<Integer> rangeError = new PublisherConcatArray<>(new PublisherRange(1, 2), new PublisherError<>(new RuntimeException("forced failure 0")));

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherRetryWhen<>(null, v -> v);
    }

    @Test(expected = NullPointerException.class)
    public void whenFactoryNull() {
        new PublisherRetryWhen<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void coldRepeater() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRetryWhen<>(justError, v -> new PublisherRange(1, 10)).subscribe(ts);
    
        ts.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void coldRepeaterBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRetryWhen<>(rangeError, v -> new PublisherRange(1, 5)).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValue(1)
        .assertNoError()
        .assertNotComplete();

        ts.request(2);
        
        ts.assertValues(1, 2, 1)
        .assertNoError()
        .assertNotComplete();

        ts.request(5);
        
        ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
        .assertNoError()
        .assertNotComplete();
        
        ts.request(10);
        
        ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1, 2)
        .assertComplete()
        .assertNoError();
    }
    
    @Test
    public void coldEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRetryWhen<>(rangeError, v -> PublisherEmpty.instance()).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void coldError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRetryWhen<>(rangeError, v -> new PublisherError<>(new RuntimeException("forced failure"))).subscribe(ts);
    
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }
    
    @Test
    public void whenFactoryThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRetryWhen<>(rangeError, v -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
    
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
    }

    @Test
    public void whenFactoryReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRetryWhen<>(rangeError, v -> null).subscribe(ts);
    
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
        
    }

    @Test
    public void repeaterErrorsInResponse() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRetryWhen<>(rangeError, v -> new PublisherMap<>(v, a -> { throw new RuntimeException("forced failure"); })).subscribe(ts);
    
        ts.assertValues(1, 2)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
        
    }

}
