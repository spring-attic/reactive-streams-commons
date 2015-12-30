package reactivestreams.commons;

import org.junit.Test;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherDistinctUntilChangedTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherDistinctUntilChanged<>(null, v -> v);
    }

    @Test(expected = NullPointerException.class)
    public void keyExtractorNull() {
        new PublisherDistinctUntilChanged<>(PublisherNever.instance(), null);
    }

    @Test
    public void allDistinct() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinctUntilChanged<>(new PublisherRange(1, 10), v -> v).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void allDistinctBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDistinctUntilChanged<>(new PublisherRange(1, 10), v -> v).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(5);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void someRepetiton() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinctUntilChanged<>(new PublisherArray<>(1, 1, 2, 2, 1, 1, 2, 2, 1, 2, 3, 3), v -> v)
          .subscribe(ts);

        ts.assertValues(1, 2, 1, 2, 1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void someRepetitionBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDistinctUntilChanged<>(new PublisherArray<>(1, 1, 2, 2, 1, 1, 2, 2, 1, 2, 3, 3), v -> v)
          .subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(4);

        ts.assertValues(1, 2, 1, 2, 1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(1, 2, 1, 2, 1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void withKeySelector() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinctUntilChanged<>(new PublisherRange(1, 10), v -> v / 3).subscribe(ts);

        ts.assertValues(1, 3, 6, 9)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void keySelectorThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinctUntilChanged<>(new PublisherRange(1, 10), v -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

}
