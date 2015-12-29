package reactivestreams.commons;

import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import reactivestreams.commons.internal.SimpleProcessor;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class PublisherTimeoutTest {

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherTimeout<>(null, () -> PublisherNever.instance(), v -> PublisherNever.instance()); 
    }

    @Test(expected = NullPointerException.class)
    public void source2Null() {
        new PublisherTimeout<>(null, () -> PublisherNever.instance(), v -> PublisherNever.instance(), PublisherNever.instance()); 
    }

    @Test(expected = NullPointerException.class)
    public void firstTimeout1Null() {
        new PublisherTimeout<>(PublisherNever.instance(), null, v -> PublisherNever.instance()); 
    }

    @Test(expected = NullPointerException.class)
    public void firstTimeout2Null() {
        new PublisherTimeout<>(PublisherNever.instance(), null, v -> PublisherNever.instance(), PublisherNever.instance()); 
    }

    @Test(expected = NullPointerException.class)
    public void itemTimeout1Null() {
        new PublisherTimeout<>(PublisherNever.instance(), () -> PublisherNever.instance(), null); 
    }

    @Test(expected = NullPointerException.class)
    public void itemTimeout2Null() {
        new PublisherTimeout<>(PublisherNever.instance(), () -> PublisherNever.instance(), null, PublisherNever.instance()); 
    }

    @Test(expected = NullPointerException.class)
    public void otherNull() {
        new PublisherTimeout<>(PublisherNever.instance(), () -> PublisherNever.instance(), v -> PublisherNever.instance(), null); 
    }

    @Test
    public void noTimeout() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherNever.instance(), v -> PublisherNever.instance()).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void immediateTimeout() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherEmpty.instance(), v -> PublisherNever.instance()).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(TimeoutException.class);
    }

    @Test
    public void firstElemenetImmediateTimeout() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherNever.instance(), v -> PublisherEmpty.instance()).subscribe(ts);
    
        ts.assertValue(1)
        .assertNotComplete()
        .assertError(TimeoutException.class);
    }

    @Test
    public void immediateTimeoutResume() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherEmpty.instance(), v -> PublisherNever.instance(), new PublisherRange(1, 10)).subscribe(ts);
    
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void firstElemenetImmediateResume() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherNever.instance(), v -> PublisherEmpty.instance(), new PublisherRange(1, 10)).subscribe(ts);
    
        ts.assertValues(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void oldTimeoutHasNoEffect() {
        SimpleProcessor<Integer> source = new SimpleProcessor<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(source, () -> tp, v -> PublisherNever.instance(), new PublisherRange(1, 10)).subscribe(ts);

        source.onNext(0);
        
        tp.onNext(1);
        
        source.onComplete();
        
        Assert.assertFalse("Timeout has subscribers?", tp.hasSubscribers());
        
        ts.assertValue(0)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void oldTimeoutCompleteHasNoEffect() {
        SimpleProcessor<Integer> source = new SimpleProcessor<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(source, () -> tp, v -> PublisherNever.instance(), new PublisherRange(1, 10)).subscribe(ts);

        source.onNext(0);
        
        tp.onComplete();
        
        source.onComplete();
        
        Assert.assertFalse("Timeout has subscribers?", tp.hasSubscribers());
        
        ts.assertValue(0)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void oldTimeoutErrorHasNoEffect() {
        SimpleProcessor<Integer> source = new SimpleProcessor<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(source, () -> tp, v -> PublisherNever.instance(), new PublisherRange(1, 10)).subscribe(ts);

        source.onNext(0);
        
        tp.onError(new RuntimeException("forced failure"));
        
        source.onComplete();
        
        Assert.assertFalse("Timeout has subscribers?", tp.hasSubscribers());
        
        ts.assertValue(0)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void firstTimeoutThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> { throw new RuntimeException("forced failure"); }, v -> PublisherNever.instance()).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void itemTimeoutThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherNever.instance(), v -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
    
        ts.assertValue(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void firstTimeoutReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> null, v -> PublisherNever.instance()).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void itemTimeoutReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherNever.instance(), v -> null).subscribe(ts);
    
        ts.assertValue(1)
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void firstTimeoutError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> new PublisherError<>(new RuntimeException("forced failure")), v -> PublisherNever.instance()).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void itemTimeoutError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), () -> PublisherNever.instance(), v -> new PublisherError<>(new RuntimeException("forced failure"))).subscribe(ts);
    
        ts.assertValue(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

}
