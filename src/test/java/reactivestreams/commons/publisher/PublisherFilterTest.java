package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;

import reactivestreams.commons.processor.UnicastProcessor;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherFilterTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherFilter<Integer>(null, e -> true);
    }

    @Test(expected = NullPointerException.class)
    public void predicateNull() {
        new PublisherFilter<>(PublisherNever.instance(), null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherFilter<>(new PublisherRange(1, 10), v -> v % 2 == 0).subscribe(ts);

        ts.assertValues(2, 4, 6, 8, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressuredRange() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);

        new PublisherFilter<>(new PublisherRange(1, 10), v -> v % 2 == 0).subscribe(ts);

        ts.assertValues(2, 4)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(2, 4, 6, 8, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressuredArray() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);

        new PublisherFilter<>(new PublisherArray<>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), v -> v % 2 == 0).subscribe(ts);

        ts.assertValues(2, 4)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(2, 4, 6, 8, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressuredIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);

        new PublisherFilter<>(new PublisherIterable<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)), v -> v % 2 == 0)
          .subscribe(ts);

        ts.assertValues(2, 4)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(2, 4, 6, 8, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void predicateThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(2);

        new PublisherFilter<>(new PublisherRange(1, 10), v -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }
    
    @Test
    public void syncFusion() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        PublisherBase.range(1, 10).filter(v -> (v & 1) == 0).subscribe(ts);
        
        ts.assertValues(2, 4, 6, 8, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusion() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        up.filter(v -> (v & 1) == 0).subscribe(ts);
        
        for (int i = 1; i < 11; i++) {
            up.onNext(i);
        }
        up.onComplete();
        
        ts.assertValues(2, 4, 6, 8, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusionBackpressured() {
        TestSubscriber<Object> ts = new TestSubscriber<>(1);

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        PublisherBase.just(1).hide().flatMap(w -> up.filter(v -> (v & 1) == 0)).subscribe(ts);
        
        up.onNext(1);
        up.onNext(2);
        
        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        up.onComplete();
        
        ts.assertValue(2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void asyncFusionBackpressured2() {
        TestSubscriber<Object> ts = new TestSubscriber<>(1);

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        new PublisherFlatMap<>(
                PublisherBase.just(1).hide(), 
                w -> up.filter(v -> (v & 1) == 0),
                false,
                1,
                PublisherBase.defaultQueueSupplier(),
                1,
                PublisherBase.defaultQueueSupplier()
        )
        .subscribe(ts);
        
        up.onNext(1);
        up.onNext(2);
        
        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        up.onComplete();
        
        ts.assertValue(2)
        .assertNoError()
        .assertComplete();
    }

}
