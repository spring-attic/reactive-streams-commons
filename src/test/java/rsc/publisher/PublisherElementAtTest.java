package rsc.publisher;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherElementAtTest {

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherElementAt<>(null, 1);
    }

    @Test(expected = NullPointerException.class)
    public void source2Null() {
        new PublisherElementAt<>(null, 1, () -> 1);
    }

    @Test(expected = NullPointerException.class)
    public void defaultSupplierNull() {
        new PublisherElementAt<>(PublisherNever.instance(), 1, null);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void indexNegative1() {
        new PublisherElementAt<>(PublisherNever.instance(), -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void indexNegative2() {
        new PublisherElementAt<>(PublisherNever.instance(), -1, () -> 1);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(new PublisherRange(1, 10), 0).subscribe(ts);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherElementAt<>(new PublisherRange(1, 10), 0).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normal2() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(new PublisherRange(1, 10), 4).subscribe(ts);

        ts.assertValue(5)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normal5Backpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherElementAt<>(new PublisherRange(1, 10), 4).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(5)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normal3() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(new PublisherRange(1, 10), 9).subscribe(ts);

        ts.assertValue(10)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normal3Backpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherElementAt<>(new PublisherRange(1, 10), 9).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(10)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(PublisherEmpty.<Integer>instance(), 0).subscribe(ts);

        ts.assertNoValues()
          .assertError(IndexOutOfBoundsException.class)
          .assertNotComplete();
    }

    @Test
    public void emptyDefault() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(PublisherEmpty.instance(), 0, () -> 20).subscribe(ts);

        ts.assertValue(20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void emptyDefaultBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherElementAt<>(PublisherEmpty.instance(), 0, () -> 20).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void nonEmptyDefault() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(new PublisherRange(1, 10), 20, () -> 20).subscribe(ts);

        ts.assertValue(20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void nonEmptyDefaultBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherElementAt<>(new PublisherRange(1, 10), 20, () -> 20).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void defaultReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(PublisherEmpty.<Integer>instance(), 0, () -> null).subscribe(ts);

        ts.assertNoValues()
          .assertError(NullPointerException.class)
          .assertNotComplete();
    }

    @Test
    public void defaultThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherElementAt<>(PublisherEmpty.<Integer>instance(), 0, () -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();
    }
}
