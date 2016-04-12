package rsc.publisher;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherRepeatPredicateTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherRepeatPredicate<>(null, () -> true);
    }

    @Test(expected = NullPointerException.class)
    public void predicateNull() {
        new PublisherRepeatPredicate<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void normal() {
        int[] times = { 1 };
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRepeatPredicate<>(new PublisherRange(1, 5), () -> times[0]-- > 0).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        int[] times = { 1 };
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherRepeatPredicate<>(new PublisherRange(1, 5), () -> times[0]-- > 0).subscribe(ts);
        
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
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void dontRepeat() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRepeatPredicate<>(new PublisherRange(1, 5), () -> false).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void predicateThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherRepeatPredicate<>(new PublisherRange(1, 5), () -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }
}
