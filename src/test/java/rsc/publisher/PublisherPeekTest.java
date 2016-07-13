package rsc.publisher;

import java.util.concurrent.atomic.*;

import org.junit.*;
import org.reactivestreams.Subscription;

import rsc.flow.Fuseable;
import rsc.processor.UnicastProcessor;
import rsc.subscriber.SubscriptionHelper;
import rsc.test.TestSubscriber;

import rsc.util.ExceptionHelper;
import rsc.util.SpscArrayQueue;

public class PublisherPeekTest {
    @Test(expected = NullPointerException.class)
    public void nullSource() {
        new PublisherPeek<>(null, null, null, null, null, null, null, null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
        AtomicReference<Integer> onNext = new AtomicReference<>();
        AtomicReference<Throwable> onError = new AtomicReference<>();
        AtomicBoolean onComplete = new AtomicBoolean();
        AtomicLong onRequest = new AtomicLong();
        AtomicBoolean onAfterComplete = new AtomicBoolean();
        AtomicBoolean onCancel = new AtomicBoolean();

        new PublisherPeek<>(new PublisherJust<>(1),
          onSubscribe::set,
          onNext::set,
          onError::set,
          () -> onComplete.set(true),
          () -> onAfterComplete.set(true),
          onRequest::set,
          () -> onCancel.set(true)
        ).subscribe(ts);

        Assert.assertNotNull(onSubscribe.get());
        Assert.assertEquals((Integer) 1, onNext.get());
        Assert.assertNull(onError.get());
        Assert.assertTrue(onComplete.get());
        Assert.assertTrue(onAfterComplete.get());
        Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
        Assert.assertFalse(onCancel.get());
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
        AtomicReference<Integer> onNext = new AtomicReference<>();
        AtomicReference<Throwable> onError = new AtomicReference<>();
        AtomicBoolean onComplete = new AtomicBoolean();
        AtomicLong onRequest = new AtomicLong();
        AtomicBoolean onAfterComplete = new AtomicBoolean();
        AtomicBoolean onCancel = new AtomicBoolean();

        new PublisherPeek<>(new PublisherError<>(new RuntimeException("forced failure")),
          onSubscribe::set,
          onNext::set,
          onError::set,
          () -> onComplete.set(true),
          () -> onAfterComplete.set(true),
          onRequest::set,
          () -> onCancel.set(true)
        ).subscribe(ts);

        Assert.assertNotNull(onSubscribe.get());
        Assert.assertNull(onNext.get());
        Assert.assertTrue(onError.get() instanceof RuntimeException);
        Assert.assertFalse(onComplete.get());
        Assert.assertTrue(onAfterComplete.get());
        Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
        Assert.assertFalse(onCancel.get());
    }

    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
        AtomicReference<Integer> onNext = new AtomicReference<>();
        AtomicReference<Throwable> onError = new AtomicReference<>();
        AtomicBoolean onComplete = new AtomicBoolean();
        AtomicLong onRequest = new AtomicLong();
        AtomicBoolean onAfterComplete = new AtomicBoolean();
        AtomicBoolean onCancel = new AtomicBoolean();

        new PublisherPeek<>(PublisherEmpty.instance(),
          onSubscribe::set,
          onNext::set,
          onError::set,
          () -> onComplete.set(true),
          () -> onAfterComplete.set(true),
          onRequest::set,
          () -> onCancel.set(true)
        ).subscribe(ts);

        Assert.assertNotNull(onSubscribe.get());
        Assert.assertNull(onNext.get());
        Assert.assertNull(onError.get());
        Assert.assertTrue(onComplete.get());
        Assert.assertTrue(onAfterComplete.get());
        Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
        Assert.assertFalse(onCancel.get());
    }

    @Test
    public void never() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
        AtomicReference<Integer> onNext = new AtomicReference<>();
        AtomicReference<Throwable> onError = new AtomicReference<>();
        AtomicBoolean onComplete = new AtomicBoolean();
        AtomicLong onRequest = new AtomicLong();
        AtomicBoolean onAfterComplete = new AtomicBoolean();
        AtomicBoolean onCancel = new AtomicBoolean();

        new PublisherPeek<>(PublisherNever.instance(),
          onSubscribe::set,
          onNext::set,
          onError::set,
          () -> onComplete.set(true),
          () -> onAfterComplete.set(true),
          onRequest::set,
          () -> onCancel.set(true)
        ).subscribe(ts);

        Assert.assertNotNull(onSubscribe.get());
        Assert.assertNull(onNext.get());
        Assert.assertNull(onError.get());
        Assert.assertFalse(onComplete.get());
        Assert.assertFalse(onAfterComplete.get());
        Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
        Assert.assertFalse(onCancel.get());
    }

    @Test
    public void neverCancel() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
        AtomicReference<Integer> onNext = new AtomicReference<>();
        AtomicReference<Throwable> onError = new AtomicReference<>();
        AtomicBoolean onComplete = new AtomicBoolean();
        AtomicLong onRequest = new AtomicLong();
        AtomicBoolean onAfterComplete = new AtomicBoolean();
        AtomicBoolean onCancel = new AtomicBoolean();

        new PublisherPeek<>(PublisherNever.instance(),
          onSubscribe::set,
          onNext::set,
          onError::set,
          () -> onComplete.set(true),
          () -> onAfterComplete.set(true),
          onRequest::set,
          () -> onCancel.set(true)
        ).subscribe(ts);

        Assert.assertNotNull(onSubscribe.get());
        Assert.assertNull(onNext.get());
        Assert.assertNull(onError.get());
        Assert.assertFalse(onComplete.get());
        Assert.assertFalse(onAfterComplete.get());
        Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
        Assert.assertFalse(onCancel.get());

        ts.cancel();

        Assert.assertTrue(onCancel.get());
    }

    @Test
    public void callbackError(){
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Throwable err = new Exception("test");

        new PublisherPeek<>(new PublisherJust<>(1),
                null,
                d -> ExceptionHelper.fail(err),
                null,
                null,
                null,
                null,
                null
        ).subscribe(ts);

        //nominal error path (DownstreamException)
        ts.assertError(err);

        ts = new TestSubscriber<>();

        try {
            new PublisherPeek<>(new PublisherJust<>(1),
                    null,
                    d -> ExceptionHelper.failUpstream(err),
                    null,
                    null,
                    null,
                    null,
                    null).subscribe(ts);

            Assert.fail();
        }
        catch (Exception e){
            //fatal publisher exception (UpstreamException)
            Assert.assertTrue(ExceptionHelper.unwrap(e) == err);
        }
    }

    @Test
    public void syncFusionAvailable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 2).doOnNext(v -> { }).subscribe(ts);
        
        Subscription s = ts.upstream();
        Assert.assertTrue("Non-fuseable upstream: " + s, s instanceof Fuseable.QueueSubscription);
    }

    @Test
    public void asyncFusionAvailable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new UnicastProcessor<Integer>(new SpscArrayQueue<>(2)).doOnNext(v -> { }).subscribe(ts);
        
        Subscription s = ts.upstream();
        Assert.assertTrue("Non-fuseable upstream" + s, s instanceof Fuseable.QueueSubscription);
    }

    @Test
    public void conditionalFusionAvailable() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        Px.wrap(u -> {
            if (!(u instanceof Fuseable.ConditionalSubscriber)) {
                SubscriptionHelper.error(u, new IllegalArgumentException("The subscriber is not " +
                        "conditional: " + u));
            } else {
                SubscriptionHelper.complete(u);
            }
        }).doOnNext(v -> { }).filter(v -> true).subscribe(ts);
        
        ts.assertNoError()
        .assertNoValues()
        .assertComplete();
    }

    @Test
    public void conditionalFusionAvailableWithFuseable() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        Px.wrapFuseable(u -> {
            if (!(u instanceof Fuseable.ConditionalSubscriber)) {
                SubscriptionHelper.error(u, new IllegalArgumentException("The subscriber is not conditional: " + u));
            } else {
                SubscriptionHelper.complete(u);
            }
        }).doOnNext(v -> { }).filter(v -> true).subscribe(ts);
        
        ts.assertNoError()
        .assertNoValues()
        .assertComplete();
    }
    
    @Test
    public void syncCompleteCalled() {
        AtomicBoolean onComplete = new AtomicBoolean();

        TestSubscriber<Object> ts = new TestSubscriber<>();

        Px.range(1, 2).doOnComplete(() -> onComplete.set(true)).subscribe(ts);
        
        ts.assertNoError()
        .assertValues(1, 2)
        .assertComplete();
        
        Assert.assertTrue("onComplete not called back", onComplete.get());
    }

    @Test
    public void syncdoAfterTerminateCalled() {
        AtomicBoolean onTerminate = new AtomicBoolean();

        TestSubscriber<Object> ts = new TestSubscriber<>();

        Px.range(1, 2).doAfterTerminate(() -> onTerminate.set(true)).subscribe(ts);
        
        ts.assertNoError()
        .assertValues(1, 2)
        .assertComplete();
        
        Assert.assertTrue("onComplete not called back", onTerminate.get());
    }

}
