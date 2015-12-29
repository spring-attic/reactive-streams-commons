package reactivestreams.commons.internal;

import org.junit.Assert;
import org.junit.Test;

import reactivestreams.commons.internal.processor.SimpleProcessor;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

public class SimpleProcessorTest {

    @Test(expected = NullPointerException.class) 
    public void onNextNull() {
        new SimpleProcessor<Integer>().onNext(null);
    }

    @Test(expected = NullPointerException.class) 
    public void onErrorNull() {
        new SimpleProcessor<Integer>().onError(null);
    }

    @Test(expected = NullPointerException.class) 
    public void onSubscribeNull() {
        new SimpleProcessor<Integer>().onSubscribe(null);
    }

    @Test(expected = NullPointerException.class) 
    public void subscribeNull() {
        new SimpleProcessor<Integer>().subscribe(null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        tp.subscribe(ts);

        Assert.assertTrue("No subscribers?", tp.hasSubscribers());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        tp.onNext(1);
        tp.onNext(2);
        
        ts.assertValues(1, 2)
        .assertNotComplete()
        .assertNoError();
        
        tp.onNext(3);
        tp.onComplete();

        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());
        
        
        ts.assertValues(1, 2, 3)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        tp.subscribe(ts);

        Assert.assertTrue("No subscribers?", tp.hasSubscribers());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(10);
        
        tp.onNext(1);
        tp.onNext(2);
        tp.onComplete();

        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void notEnoughRequests() {

        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        tp.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        tp.onNext(1);
        tp.onNext(2);
        tp.onComplete();
        
        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertValues(1)
        .assertError(IllegalStateException.class)
        .assertNotComplete();
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        tp.subscribe(ts);

        Assert.assertTrue("No subscribers?", tp.hasSubscribers());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        tp.onNext(1);
        tp.onNext(2);
        
        ts.assertValues(1, 2)
        .assertNotComplete()
        .assertNoError();
        
        tp.onNext(3);
        tp.onError(new RuntimeException("forced failure"));

        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNotNull("Has error?", tp.getError());
        Assert.assertTrue("No error?", tp.hasError());
        
        Throwable e = tp.getError();
        Assert.assertTrue("Wrong exception? " + e, RuntimeException.class.isInstance(e));
        Assert.assertEquals("forced failure", e.getMessage());
        
        ts.assertValues(1, 2, 3)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void terminatedWithError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        tp.onError(new RuntimeException("forced failure"));
        
        tp.subscribe(ts);

        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNotNull("No error?", tp.getError());
        Assert.assertTrue("No error?", tp.hasError());

        Throwable e = tp.getError();
        Assert.assertTrue("Wrong exception? " + e, RuntimeException.class.isInstance(e));
        Assert.assertEquals("forced failure", e.getMessage());
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void terminatedNormally() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        tp.onComplete();
        
        tp.subscribe(ts);

        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());
        
        ts.assertNoValues()
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void subscriberAlreadyCancelled() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.cancel();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        tp.subscribe(ts);

        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());

        tp.onNext(1);
        
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertNoError();
    }

    @Test
    public void subscriberCancels() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        tp.subscribe(ts);

        Assert.assertTrue("No Subscribers present?", tp.hasSubscribers());

        tp.onNext(1);

        ts.assertValue(1)
        .assertNoError()
        .assertNotComplete();
        
        ts.cancel();

        Assert.assertFalse("Subscribers present?", tp.hasSubscribers());

        tp.onNext(2);
        
        ts.assertValue(1)
        .assertNotComplete()
        .assertNoError();
    }

}
