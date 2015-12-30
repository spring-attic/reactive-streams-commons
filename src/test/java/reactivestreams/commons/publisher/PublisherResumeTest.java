package reactivestreams.commons.publisher;

import org.junit.Test;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.publisher.PublisherError;
import reactivestreams.commons.publisher.PublisherNever;
import reactivestreams.commons.publisher.PublisherRange;
import reactivestreams.commons.publisher.PublisherResume;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherResumeTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherResume<>(null, v -> PublisherNever.instance());
    }

    @Test(expected = NullPointerException.class)
    public void nextFactoryNull() {
        new PublisherResume<>(PublisherNever.instance(), null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherResume<>(new PublisherRange(1, 10), v -> new PublisherRange(11, 10)).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherResume<>(new PublisherRange(1, 10), v -> new PublisherRange(11, 10)).subscribe(ts);

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
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherResume<>(new PublisherError<>(new RuntimeException("forced failure")), v -> new PublisherRange
          (11, 10)).subscribe(ts);

        ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void errorBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherResume<>(new PublisherError<>(new RuntimeException("forced failure")), v -> new PublisherRange
          (11, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(2);

        ts.assertValues(11, 12)
          .assertNoError()
          .assertNotComplete();

        ts.request(5);

        ts.assertValues(11, 12, 13, 14, 15, 16, 17)
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void someFirst() {
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherResume<>(tp, v -> new PublisherRange(11, 10)).subscribe(ts);

        tp.onNext(1);
        tp.onNext(2);
        tp.onNext(3);
        tp.onNext(4);
        tp.onNext(5);
        tp.onError(new RuntimeException("forced failure"));

        ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void someFirstBackpressured() {
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();

        TestSubscriber<Integer> ts = new TestSubscriber<>(10);

        new PublisherResume<>(tp, v -> new PublisherRange(11, 10)).subscribe(ts);

        tp.onNext(1);
        tp.onNext(2);
        tp.onNext(3);
        tp.onNext(4);
        tp.onNext(5);
        tp.onError(new RuntimeException("forced failure"));

        ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15)
          .assertNotComplete()
          .assertNoError();

        ts.request(10);

        ts.assertValues(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void nextFactoryThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherResume<Integer>(new PublisherError<>(new RuntimeException("forced failure")), v -> {
            throw new RuntimeException("forced failure 2");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure 2");
    }

    @Test
    public void nextFactoryReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherResume<Integer>(new PublisherError<>(new RuntimeException("forced failure")), v -> null)
          .subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

}
