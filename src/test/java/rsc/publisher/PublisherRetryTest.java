package rsc.publisher;

import java.util.function.Consumer;

import org.junit.Test;
import org.reactivestreams.Publisher;

import rsc.test.TestSubscriber;

public class PublisherRetryTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherRetry<>(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void timesInvalid() {
        new PublisherRetry<>(PublisherNever.instance(), -1);
    }

    @Test
    public void zeroRetryNoError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRetry<>(new PublisherRange(1, 10), 0).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    final Publisher<Integer> source = Px.concatArray(new PublisherRange(1, 3), new PublisherError<>(new
      RuntimeException("forced failure")));

    @Test
    public void zeroRetry() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRetry<>(source, 0).subscribe(ts);

        ts.assertValues(1, 2, 3)
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void oneRetry() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherRetry<>(source, 1).subscribe(ts);

        ts.assertValues(1, 2, 3, 1, 2, 3)
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void oneRetryBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(4);

        new PublisherRetry<>(source, 1).subscribe(ts);

        ts.assertValues(1, 2, 3, 1)
          .assertNotComplete()
          .assertNoError();
    }

    @Test
    public void retryInfinite() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTake<>(new PublisherRetry<>(source), 10).subscribe(ts);

        ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
          .assertComplete()
          .assertNoError();

    }

    @Test
    public void doOnNextFails() {
        Px.just(1)
        .doOnNext(new Consumer<Integer>() {
            int i;
            @Override
            public void accept(Integer t) {
                if (i++ < 2) {
                    throw new RuntimeException("test");
                }
            }
        })
        .retry(2)
        .test()
        .assertResult(1);
    }
}
