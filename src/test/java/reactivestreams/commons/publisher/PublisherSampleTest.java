package reactivestreams.commons.publisher;

import org.junit.Assert;
import org.junit.Test;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.test.TestSubscriber;

public class PublisherSampleTest {

    @Test(expected = NullPointerException.class)
    public void sourceNull() {
        new PublisherSample<>(null, PublisherNever.instance());
    }

    @Test(expected = NullPointerException.class)
    public void otherNull() {
        new PublisherSample<>(PublisherNever.instance(), null);
    }

    void sample(boolean complete, boolean which) {
        SimpleProcessor<Integer> main = new SimpleProcessor<>();

        SimpleProcessor<String> other = new SimpleProcessor<>();

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSample<>(main, other).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        main.onNext(1);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        other.onNext("first");

        ts.assertValue(1)
          .assertNoError()
          .assertNotComplete();

        other.onNext("second");

        ts.assertValue(1)
          .assertNoError()
          .assertNotComplete();

        main.onNext(2);

        ts.assertValue(1)
          .assertNoError()
          .assertNotComplete();

        other.onNext("third");

        ts.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        SimpleProcessor<?> p = which ? main : other;

        if (complete) {
            p.onComplete();

            ts.assertValues(1, 2)
              .assertComplete()
              .assertNoError();
        } else {
            p.onError(new RuntimeException("forced failure"));

            ts.assertValues(1, 2)
              .assertNotComplete()
              .assertError(RuntimeException.class)
              .assertErrorMessage("forced failure");
        }

        Assert.assertFalse("Main has subscribers?", main.hasSubscribers());
        Assert.assertFalse("Other has subscribers?", other.hasSubscribers());
    }

    @Test
    public void normal1() {
        sample(true, false);
    }

    @Test
    public void normal2() {
        sample(true, true);
    }

    @Test
    public void error1() {
        sample(false, false);
    }

    @Test
    public void error2() {
        sample(false, true);
    }

    @Test
    public void subscriberCancels() {
        SimpleProcessor<Integer> main = new SimpleProcessor<>();

        SimpleProcessor<String> other = new SimpleProcessor<>();

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSample<>(main, other).subscribe(ts);

        Assert.assertTrue("Main no subscriber?", main.hasSubscribers());
        Assert.assertTrue("Other no subscriber?", other.hasSubscribers());

        ts.cancel();

        Assert.assertFalse("Main no subscriber?", main.hasSubscribers());
        Assert.assertFalse("Other no subscriber?", other.hasSubscribers());

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();
    }

    public void completeImmediately(boolean which) {
        SimpleProcessor<Integer> main = new SimpleProcessor<>();

        SimpleProcessor<String> other = new SimpleProcessor<>();

        if (which) {
            main.onComplete();
        } else {
            other.onComplete();
        }

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSample<>(main, other).subscribe(ts);

        Assert.assertFalse("Main subscriber?", main.hasSubscribers());
        Assert.assertFalse("Other subscriber?", other.hasSubscribers());

        ts.assertNoValues()
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void mainCompletesImmediately() {
        completeImmediately(true);
    }

    @Test
    public void otherCompletesImmediately() {
        completeImmediately(false);
    }

}
