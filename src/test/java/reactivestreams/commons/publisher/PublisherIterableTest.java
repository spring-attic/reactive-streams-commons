package reactivestreams.commons.publisher;

import java.util.*;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherIterableTest {

    final Iterable<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    @Test(expected = NullPointerException.class)
    public void nullIterable() {
        new PublisherIterable<Integer>(null);
    }

    @Test
    public void nullIterator() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherIterable<Integer>(() -> null).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherIterable<>(source).subscribe(ts);

        ts
          .assertValueSequence(source)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherIterable<>(source).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(5);

        ts
          .assertValues(1, 2, 3, 4, 5)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts
          .assertValueSequence(source)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressuredExact() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(10);

        new PublisherIterable<>(source).subscribe(ts);

        ts
          .assertValueSequence(source)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void iteratorReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherIterable<>(Arrays.asList(1, 2, 3, 4, 5, null, 7, 8, 9, 10)).subscribe(ts);

        ts
          .assertValues(1, 2, 3, 4, 5)
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void emptyMapped() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        Px.fromIterable(Collections.<Integer>emptyList()).map(v -> v + 1).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

}
