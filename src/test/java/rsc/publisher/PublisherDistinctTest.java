package rsc.publisher;

import java.util.HashSet;

import org.junit.Test;
import reactor.core.flow.Fuseable;
import rsc.processor.ReplayProcessor;
import rsc.test.TestSubscriber;

public class PublisherDistinctTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherDistinct<>(null, k -> k, HashSet::new);
    }

    @Test(expected = NullPointerException.class)
    public void keyExtractorNull() {
        new PublisherDistinct<>(PublisherNever.instance(), null, HashSet::new);
    }

    @Test(expected = NullPointerException.class)
    public void collectionSupplierNull() {
        new PublisherDistinct<>(PublisherNever.instance(), k -> k, null);
    }

    @Test
    public void allDistinct() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinct<>(new PublisherRange(1, 10), k -> k, HashSet::new).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void allDistinctBackpressured() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDistinct<>(new PublisherRange(1, 10), k -> k, HashSet::new).subscribe(ts);

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
    public void someDistinct() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinct<>(new PublisherArray<>(1, 2, 2, 3, 4, 5, 6, 1, 2, 7, 7, 8, 9, 9, 10, 10, 10), k -> k,
          HashSet::new).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void someDistinctBackpressured() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDistinct<>(new PublisherArray<>(1, 2, 2, 3, 4, 5, 6, 1, 2, 7, 7, 8, 9, 9, 10, 10, 10), k -> k,
          HashSet::new).subscribe(ts);

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
    public void allSame() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinct<>(new PublisherArray<>(1, 1, 1, 1, 1, 1, 1, 1, 1), k -> k, HashSet::new).subscribe(ts);

        ts.assertValue(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void allSameFusable() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ANY);

        new PublisherDistinct<>(new PublisherArray<>(1, 1, 1, 1, 1, 1, 1, 1, 1),
                k -> k,
                HashSet::new).filter(t -> true)
                             .map(t -> t)
                             .process(new ReplayProcessor<>(4, false))
                             .autoConnect()
                             .subscribe(ts);

        ts.assertValue(1)
          .assertFuseableSource()
          .assertFusionEnabled()
          .assertFusionMode(Fuseable.ASYNC)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void allSameBackpressured() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDistinct<>(new PublisherArray<>(1, 1, 1, 1, 1, 1, 1, 1, 1), k -> k, HashSet::new).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValue(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void withKeyExtractorSame() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinct<>(new PublisherRange(1, 10), k -> k % 3, HashSet::new).subscribe(ts);

        ts.assertValues(1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void withKeyExtractorBackpressured() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDistinct<>(new PublisherRange(1, 10), k -> k % 3, HashSet::new).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(1, 2)
          .assertNotComplete()
          .assertNoError();

        ts.request(2);

        ts.assertValues(1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void keyExtractorThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinct<>(new PublisherRange(1, 10), k -> {
            throw new RuntimeException("forced failure");
        }, HashSet::new).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void collectionSupplierThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinct<>(new PublisherRange(1, 10), k -> k, () -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void collectionSupplierReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDistinct<>(new PublisherRange(1, 10), k -> k, () -> null).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }
}
