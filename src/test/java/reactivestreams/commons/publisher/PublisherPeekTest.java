package reactivestreams.commons.publisher;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ExceptionHelper;

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

        new PublisherPeek<>(new PublisherError<Integer>(new RuntimeException("forced failure")),
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

}
