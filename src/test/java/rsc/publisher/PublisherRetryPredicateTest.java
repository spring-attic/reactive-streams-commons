package rsc.publisher;

import org.junit.Test;
import org.reactivestreams.Publisher;
import rsc.test.TestSubscriber;

public class PublisherRetryPredicateTest {
    
    final Publisher<Integer> source = new PublisherConcatArray<>(new PublisherRange(1, 5), new PublisherError<Integer>(new RuntimeException("forced failure 0")));
    
    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherRetryPredicate<>(null, e -> true);
    }

    @Test(expected = NullPointerException.class)
    public void predicateNull() {
        new PublisherRetryPredicate<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void normal() {
        int[] times = { 1 };
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRetryPredicate<>(source, e -> times[0]-- > 0).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure 0")
        .assertNotComplete();
    }

    @Test
    public void normalBackpressured() {
        int[] times = { 1 };
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRetryPredicate<>(source, e -> times[0]-- > 0).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts.request(5);

        ts.assertValues(1, 2, 3, 4, 5, 1, 2)
        .assertNoError()
        .assertNotComplete();
        
        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure 0")
        .assertNotComplete();
    }

    @Test
    public void dontRepeat() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRetryPredicate<>(source, e -> false).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure 0")
        .assertNotComplete();
    }

    @Test
    public void predicateThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRetryPredicate<>(source, e -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }
}
