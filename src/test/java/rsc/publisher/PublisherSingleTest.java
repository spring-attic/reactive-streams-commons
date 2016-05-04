package rsc.publisher;

import java.util.NoSuchElementException;

import org.junit.Test;

import rsc.flow.Fuseable;
import rsc.test.TestSubscriber;

public class PublisherSingleTest {

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherSingle<>(null);
    }

    @Test(expected = NullPointerException.class)
    public void source2Null() {
        new PublisherSingle<>(null, () -> 1);
    }

    @Test(expected = NullPointerException.class)
    public void defaultSupplierNull() {
        new PublisherSingle<>(PublisherNever.instance(), null);
    }

    @Test
    public void defaultReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSingle<>(PublisherEmpty.<Integer>instance(), () -> null).subscribe(ts);

        ts.assertNoValues()
          .assertError(NullPointerException.class)
          .assertNotComplete();
    }

    @Test
    public void defaultThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSingle<>(PublisherEmpty.<Integer>instance(), () -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();
    }

    @Test
    public void normal() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSingle<>(new PublisherJust<>(1)).subscribe(ts);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSingle<>(new PublisherJust<>(1)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void empty() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSingle<>(PublisherEmpty.<Integer>instance()).subscribe(ts);

        ts.assertNoValues()
          .assertError(NoSuchElementException.class)
          .assertNotComplete();
    }

    @Test
    public void emptyDefault() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSingle<>(PublisherEmpty.instance(), () -> 1).subscribe(ts);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void emptyDefaultBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSingle<>(PublisherEmpty.instance(), () -> 1).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void multi() {

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherSingle<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertError(IndexOutOfBoundsException.class)
          .assertNotComplete();
    }

    @Test
    public void multiBackpressured() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);

        new PublisherSingle<>(new PublisherRange(1, 10)).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertNoValues()
          .assertError(IndexOutOfBoundsException.class)
          .assertNotComplete();
    }

    @Test
    public void fused() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Px.just(1).hide().single().subscribe(ts);
        
        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertResult(1)
        ;
        
    }
}
