package reactivestreams.commons.publisher;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.subscriber.test.TestSubscriber;
import reactivestreams.commons.util.EmptySubscription;

public class PublisherTakeTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherTake<>(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void numberIsInvalid() {
        new PublisherTake<>(PublisherNever.instance(), -1);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTake<>(new PublisherRange(1, 10), 5).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTake<>(new PublisherRange(1, 10), 5).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void takeZero() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherTake<>(new PublisherRange(1, 10), 0).subscribe(ts);

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }
    
    @Test
    public void takeOverflowAttempt() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Publisher<Integer> p = s -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            s.onNext(1);
            s.onNext(2);
            s.onNext(3);
        };
        
        new PublisherTake<>(p, 2).subscribe(ts);
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();
    }
}
