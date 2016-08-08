package rsc.publisher;

import org.junit.Test;
import org.reactivestreams.Publisher;
import rsc.test.TestSubscriber;

public class PublisherConcatArrayTest {

    @Test(expected = NullPointerException.class)
    public void arrayNull() {
        Px.concatArray((Publisher<Object>[]) null);
    }

    final Publisher<Integer> source = new PublisherRange(1, 3);

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.concatArray(source, source, source).subscribe(ts);

        ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        Px.concatArray(source, source, source).subscribe(ts);

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

        Px.concatArray(source, null, source).subscribe(ts);

        ts.assertValues(1, 2, 3)
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void singleSourceIsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.concatArray((Publisher<Integer>) null).subscribe(ts);

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

    @Test
    public void errorDelayed() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.concatArray(true, Px.range(1, 2), Px.error(new RuntimeException("Forced failure")), Px.range(3, 2))
        .subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4)
        .assertError(RuntimeException.class)
        .assertErrorMessage("Forced failure")
        .assertNotComplete();
    }

    @Test
    public void errorManyDelayed() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.concatArray(true, Px.range(1, 2), 
                Px.error(new RuntimeException("Forced failure")), 
                Px.range(3, 2),
                Px.error(new RuntimeException("Forced failure")), 
                Px.empty())
        .subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4)
        .assertError(Throwable.class)
        .assertErrorMessage("Multiple exceptions")
        .assertNotComplete();
    }

    @Test
    public void veryLongTake() {
        Px.range(1, 1_000_000_000).concatWith(Px.<Integer>empty()).take(10)
        .test()
        .assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
}
