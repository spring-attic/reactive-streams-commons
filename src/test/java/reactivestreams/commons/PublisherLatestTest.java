package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.TestProcessor;
import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherLatestTest {
    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherLatest<>(null);
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherLatest<>(new PublisherRange(1, 10)).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void backpressured() {
        TestProcessor<Integer> tp = new TestProcessor<>();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherLatest<>(tp).subscribe(ts);
        
        tp.onNext(1);
        
        ts.assertNoValues().assertNoError().assertNotComplete();
        
        tp.onNext(2);
        
        ts.request(1);

        ts.assertValue(2).assertNoError().assertNotComplete();

        tp.onNext(3);
        tp.onNext(4);

        ts.request(2);

        ts.assertValues(2, 4).assertNoError().assertNotComplete();

        tp.onNext(5);
        tp.onComplete();
        
        ts.assertValues(2, 4, 5).assertNoError().assertComplete();
    }
    
    @Test
    public void error() {
        TestProcessor<Integer> tp = new TestProcessor<>();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherLatest<>(tp).subscribe(ts);

        tp.onError(new RuntimeException("forced failure"));
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }
}
