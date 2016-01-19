package reactivestreams.commons.publisher;

import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.subscriber.test.TestSubscriber;
import reactivestreams.commons.support.ConstructorTestBuilder;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Processors;
import reactor.core.subscription.ReactiveSession;

public class PublisherTimeoutTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherTimeout.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("firstTimeout", PublisherNever.instance());
        ctb.addRef("itemTimeout", (Function<Object, Publisher<Object>>)v -> PublisherNever.instance());
        ctb.addRef("other", PublisherNever.instance());
        
        ctb.test();
    }

    @Test
    public void noTimeout() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherNever.instance(), v -> PublisherNever
          .instance()).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void immediateTimeout() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherEmpty.instance(), v -> PublisherNever
          .instance()).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(TimeoutException.class);
    }

    @Test
    public void firstElemenetImmediateTimeout() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherNever.instance(), v -> PublisherEmpty
          .instance()).subscribe(ts);

        ts.assertValue(1)
          .assertNotComplete()
          .assertError(TimeoutException.class);
    }

    @Test
    public void immediateTimeoutResume() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherEmpty.instance(), v -> PublisherNever
          .instance(), new PublisherRange(1, 10)).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void firstElemenetImmediateResume() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherNever.instance(), v -> PublisherEmpty
          .instance(), new PublisherRange(1, 10)).subscribe(ts);

        ts.assertValues(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void oldTimeoutHasNoEffect() {
        SimpleProcessor<Integer> source = new SimpleProcessor<>();

        SimpleProcessor<Integer> tp = new SimpleProcessor<>();

        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(source, tp, v -> PublisherNever.instance(), new PublisherRange(1, 10)).subscribe
          (ts);

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

        new PublisherTimeout<>(source, tp, v -> PublisherNever.instance(), new PublisherRange(1, 10)).subscribe
          (ts);

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

        new PublisherTimeout<>(source, tp, v -> PublisherNever.instance(), new PublisherRange(1, 10)).subscribe
          (ts);

        source.onNext(0);

        tp.onError(new RuntimeException("forced failure"));

        source.onComplete();

        Assert.assertFalse("Timeout has subscribers?", tp.hasSubscribers());

        ts.assertValue(0)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void itemTimeoutThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherNever.instance(), v -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertValue(1)
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void itemTimeoutReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherNever.instance(), v -> null).subscribe(ts);

        ts.assertValue(1)
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void firstTimeoutError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), new PublisherError<>(new RuntimeException("forced " +
          "failure")), v -> PublisherNever.instance()).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void itemTimeoutError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherTimeout<>(new PublisherRange(1, 10), PublisherNever.instance(), v -> new PublisherError<>
          (new RuntimeException("forced failure"))).subscribe(ts);

        ts.assertValue(1)
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void concurrentTimeoutError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        FluxProcessor<Integer, Integer> p = Processors.queue();
        FluxProcessor<Integer, Integer> p2 = Processors.queue("test", 16);

        p2.subscribe(ts);

        ReactiveSession<Integer> s = p.startSession();
//        new PublisherTake<>(p, 100)
//                .doOnRequest(r -> System.out.println("-- " + r))
//                .doOnNext(System.out::println)
//                .timeout(PublisherNever.instance(), v -> PublisherNever.instance())
//                .subscribe(p2);
        p.subscribe(p2);


        for(int i = 0; i < 100; i++) {
            s.submit(1);
        }
        p.onComplete();

        ts.await();
        ts.assertValueCount(100).assertComplete();
    }

    @Test
    public void timeoutRequested() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        SimpleProcessor<Integer> source = new SimpleProcessor<>();

        SimpleProcessor<Integer> tp = new SimpleProcessor<>();
        
        source.timeout(tp, v -> tp).subscribe(ts);
        
        tp.onNext(1);
        
        source.onNext(2);
        source.onComplete();
        
        ts.assertNoValues()
        .assertError(TimeoutException.class)
        .assertNotComplete();
    }
}
