package reactivestreams.commons;

import org.junit.Assert;
import org.junit.Test;
import reactivestreams.commons.internal.processor.SimpleProcessor;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PublisherDropTest {

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherDrop<>(null);
    }

    @Test(expected = NullPointerException.class)
    public void source2Null() {
        new PublisherDrop<>(null, v -> {
        });
    }

    @Test(expected = NullPointerException.class)
    public void onDropNull() {
        new PublisherDrop<>(PublisherNever.instance(), null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDrop<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDrop<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void someDrops() {
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        List<Integer> drops = new ArrayList<>();

        new PublisherDrop<>(tp, e -> drops.add(e)).subscribe(ts);

        tp.onNext(1);

        ts.request(2);

        tp.onNext(2);
        tp.onNext(3);
        tp.onNext(4);

        ts.request(1);

        tp.onNext(5);
        tp.onComplete();

        ts.assertValues(2, 3, 5)
          .assertComplete()
          .assertNoError();

        Assert.assertEquals(Arrays.asList(1, 4), drops);
    }

    @Test
    public void onDropThrows() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherDrop<>(new PublisherRange(1, 10), e -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }
}
