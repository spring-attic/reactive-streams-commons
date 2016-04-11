package reactivestreams.commons.publisher;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherConcatArrayTest {

    @Test(expected = NullPointerException.class)
    public void arrayNull() {
        new PublisherConcatArray<>((Publisher<Object>[]) null);
    }

    final Publisher<Integer> source = new PublisherRange(1, 3);

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherConcatArray<>(source, source, source).subscribe(ts);

        ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherConcatArray<>(source, source, source).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertNoError()
          .assertNotComplete();

        ts.request(4);

        ts.assertValues(1, 2, 3, 1, 2)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void oneSourceIsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherConcatArray<>(source, null, source).subscribe(ts);

        ts.assertValues(1, 2, 3)
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void singleSourceIsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherConcatArray<>((Publisher<Integer>) null).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void scalarAndRangeBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.just(1).concatWith(Px.range(2, 3)).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError();
        
        ts.request(5);
        
        ts.assertValues(1, 2, 3, 4)
        .assertComplete()
        .assertNoError();
    }

}
