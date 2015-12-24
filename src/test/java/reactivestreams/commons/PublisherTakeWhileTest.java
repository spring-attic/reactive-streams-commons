package reactivestreams.commons;

import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherTakeWhileTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherTakeWhile<>(null, v -> true);
    }
    
    @Test(expected = NullPointerException.class)
    public void predicateNull() {
        new PublisherTakeWhile<>(PublisherNever.instance(), null);
    }
    
    @Test
    public void takeAll() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeWhile<>(new PublisherRange(1, 5), v -> true).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void takeAllBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherTakeWhile<>(new PublisherRange(1, 5), v -> true).subscribe(ts);
    
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
    public void takeSome() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeWhile<>(new PublisherRange(1, 5), v -> v < 4).subscribe(ts);
    
        ts.assertValues(1, 2, 3)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void takeSomeBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherTakeWhile<>(new PublisherRange(1, 5), v -> v < 4).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts.request(10);
        
        ts.assertValues(1, 2, 3)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void takeNone() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeWhile<>(new PublisherRange(1, 5), v -> false).subscribe(ts);;
    
        ts.assertNoValues()
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void takeNoneBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherTakeWhile<>(new PublisherRange(1, 5), v -> false).subscribe(ts);
    
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);

        ts.assertNoValues()
        .assertComplete()
        .assertNoError();
    }
    
    @Test
    public void predicateThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherTakeWhile<>(new PublisherRange(1, 5), v -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
        
    }

}
