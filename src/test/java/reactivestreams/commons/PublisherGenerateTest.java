package reactivestreams.commons;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherGenerateTest {

    @Test(expected = NullPointerException.class)
    public void stateSupplierNull() {
        new PublisherGenerate<>(null, (s, o) -> s, s -> { });
    }

    @Test(expected = NullPointerException.class)
    public void generatorNull() {
        new PublisherGenerate<>(() -> 1, null, s -> { });
    }

    @Test(expected = NullPointerException.class)
    public void stateConsumerNull() {
        new PublisherGenerate<>(() -> 1, (s, o) -> s, null);
    }

    @Test
    public void generateEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Void>((s, o) -> {
            o.onComplete();
            return s;
        }).subscribe(ts);

        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void generateNever() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Void>((s, o) -> {
            o.stop();
            return s;
        }).subscribe(ts);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
    }

    @Test
    public void generateJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Void>((s, o) -> {
            o.onNext(1);
            o.onComplete();
            return s;
        }).subscribe(ts);

        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void generateError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Void>((s, o) -> {
            o.onError(new RuntimeException("forced failure"));
            return s;
        }).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        ;
    }

    
    @Test
    public void generateJustBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherGenerate<Integer, Void>((s, o) -> {
            o.onNext(1);
            o.onComplete();
            return s;
        }).subscribe(ts);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void generateRange() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Integer>(() -> 1, (s, o) -> {
            if (s < 11) {
                o.onNext(s);
            } else {
                o.onComplete();
            }
            return s + 1;
        }).subscribe(ts);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void generateRangeBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherGenerate<Integer, Integer>(() -> 1, (s, o) -> {
            if (s < 11) {
                o.onNext(s);
            } else {
                o.onComplete();
            }
            return s + 1;
        }).subscribe(ts);

        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(2);
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts.request(10);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void stateSupplierThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Integer>(() -> { throw new RuntimeException("forced failure"); }, (s, o) -> { o.onNext(1); return s; }).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void generatorThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Integer>((s, o) -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void generatorMultipleOnErrors() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Integer>((s, o) -> { 
            o.onError(new RuntimeException("forced failure")); 
            o.onError(new RuntimeException("forced failure")); 
            return s;
        }).subscribe(ts);
    
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void generatorMultipleOnCompletes() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Integer>((s, o) -> { 
            o.onComplete();
            o.onComplete();
            return s;
        }).subscribe(ts);
    
        ts.assertNoValues()
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void generatorMultipleOnNexts() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherGenerate<Integer, Integer>((s, o) -> { o.onNext(1); o.onNext(1); return s; }).subscribe(ts);
    
        ts.assertValue(1)
        .assertNotComplete()
        .assertError(IllegalStateException.class);
    }

    @Test
    public void stateConsumerCalled() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        AtomicInteger stateConsumer = new AtomicInteger();
        
        new PublisherGenerate<Integer, Integer>(() -> 1, (s, o) -> { o.onComplete(); return s; }, stateConsumer::set).subscribe(ts);
    
        ts.assertNoValues()
        .assertComplete()
        .assertNoError();
        
        Assert.assertEquals(1, stateConsumer.get());
    }
    
    @Test
    public void iterableSource() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
        new PublisherGenerate<Integer, Iterator<Integer>>(
                () -> list.iterator(),
                (s, o) -> {
                    if (s.hasNext()) {
                        o.onNext(s.next());
                    } else {
                        o.onComplete();
                    }
                    return s;
                }).subscribe(ts);
        
        ts.assertValueSequence(list)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void iterableSourceBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
        new PublisherGenerate<Integer, Iterator<Integer>>(
                () -> list.iterator(),
                (s, o) -> {
                    if (s.hasNext()) {
                        o.onNext(s.next());
                    } else {
                        o.onComplete();
                    }
                    return s;
                }).subscribe(ts);
        
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
        ts.assertValueSequence(list)
        .assertComplete()
        .assertNoError();
    }
}
