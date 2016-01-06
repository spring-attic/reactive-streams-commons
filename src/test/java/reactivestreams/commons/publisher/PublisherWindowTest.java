package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactivestreams.commons.processor.*;
import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherWindowTest {

    // javac can't handle these inline and fails with type inference error
    final Supplier<Queue<Integer>> pqs = ConcurrentLinkedQueue::new;
    final Supplier<Queue<UnicastProcessor<Integer>>> oqs = ConcurrentLinkedQueue::new;

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherWindow<>(null, 1, pqs);
    }

    @Test(expected = NullPointerException.class)
    public void source2Null() {
        new PublisherWindow<>(null, 1, 2, pqs, oqs);
    }

    @Test(expected = NullPointerException.class)
    public void processorQueue1Null() {
        new PublisherWindow<>(PublisherNever.<Object>instance(), 1, null);
    }

    @Test(expected = NullPointerException.class)
    public void processorQueue2Null() {
        new PublisherWindow<>(PublisherNever.<Integer>instance(), 1, 1, null, oqs);
    }

    @Test(expected = NullPointerException.class)
    public void overflowQueueNull() {
        new PublisherWindow<>(PublisherNever.<Integer>instance(), 1, 1, pqs, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void size1Invalid() {
        new PublisherWindow<>(PublisherNever.<Integer>instance(), 0, pqs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void size2Invalid() {
        new PublisherWindow<>(PublisherNever.<Integer>instance(), 0, 2, pqs, oqs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void skipInvalid() {
        new PublisherWindow<>(PublisherNever.instance(), 1, 0, pqs, oqs);
    }

    @Test
    public void processorQueue1ReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherWindow<>(new PublisherRange(1, 10), 1, () -> null).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void processorQueue2ReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherWindow<>(new PublisherRange(1, 10), 1, 2, () -> null, oqs).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void overflowQueueReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherWindow<>(new PublisherRange(1, 10), 2, 1, pqs, () -> null).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    static <T> Supplier<T> throwing() {
        return () -> { throw new RuntimeException("forced failure"); };
    }

    @Test
    public void processorQueue1Throws() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherWindow<>(new PublisherRange(1, 10), 1, throwing()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void processorQueue2Throws() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherWindow<>(new PublisherRange(1, 10), 1, 2, throwing(), oqs).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void overflowQueueThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherWindow<>(new PublisherRange(1, 10), 2, 1, pqs, throwing()).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    static <T> TestSubscriber<T> toList(Publisher<T> windows) {
        TestSubscriber<T> ts = new TestSubscriber<>();
        windows.subscribe(ts);
        return ts;
    }
    
    @Test
    public void exact() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        new PublisherWindow<>(new PublisherRange(1, 10), 3, pqs).subscribe(ts);
        
        ts.assertValueCount(4)
        .assertComplete()
        .assertNoError();
        
        toList(ts.values().get(0))
        .assertValues(1, 2, 3)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(1))
        .assertValues(4, 5, 6)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(2))
        .assertValues(7, 8, 9)
        .assertComplete()
        .assertNoError();
        
        toList(ts.values().get(3))
        .assertValues(10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void exactBackpressured() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>(0L);
        
        new PublisherWindow<>(new PublisherRange(1, 10), 3, pqs).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValueCount(1)
        .assertNotComplete()
        .assertNoError();
        
        toList(ts.values().get(0))
        .assertValues(1, 2, 3)
        .assertComplete()
        .assertNoError();

        ts.request(1);
        
        ts.assertValueCount(2)
        .assertNotComplete()
        .assertNoError();
        
        toList(ts.values().get(1))
        .assertValues(4, 5, 6)
        .assertComplete()
        .assertNoError();

        ts.request(1);
        
        ts.assertValueCount(3)
        .assertNotComplete()
        .assertNoError();

        toList(ts.values().get(2))
        .assertValues(7, 8, 9)
        .assertComplete()
        .assertNoError();

        ts.request(1);
        
        ts.assertValueCount(4)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(3))
        .assertValues(10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void exactWindowCount() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        new PublisherWindow<>(new PublisherRange(1, 9), 3, pqs).subscribe(ts);
        
        ts.assertValueCount(3)
        .assertComplete()
        .assertNoError();
        
        toList(ts.values().get(0))
        .assertValues(1, 2, 3)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(1))
        .assertValues(4, 5, 6)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(2))
        .assertValues(7, 8, 9)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void skip() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        new PublisherWindow<>(new PublisherRange(1, 10), 2, 3, pqs, oqs).subscribe(ts);
        
        ts.assertValueCount(4)
        .assertComplete()
        .assertNoError();
        
        toList(ts.values().get(0))
        .assertValues(1, 2)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(1))
        .assertValues(4, 5)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(2))
        .assertValues(7, 8)
        .assertComplete()
        .assertNoError();
        
        toList(ts.values().get(3))
        .assertValues(10)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void skipBackpressured() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>(0L);
        
        new PublisherWindow<>(new PublisherRange(1, 10), 2, 3, pqs, oqs).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValueCount(1)
        .assertNotComplete()
        .assertNoError();
        
        toList(ts.values().get(0))
        .assertValues(1, 2)
        .assertComplete()
        .assertNoError();

        ts.request(1);
        
        ts.assertValueCount(2)
        .assertNotComplete()
        .assertNoError();
        
        toList(ts.values().get(1))
        .assertValues(4, 5)
        .assertComplete()
        .assertNoError();

        ts.request(1);
        
        ts.assertValueCount(3)
        .assertNotComplete()
        .assertNoError();

        toList(ts.values().get(2))
        .assertValues(7, 8)
        .assertComplete()
        .assertNoError();

        ts.request(1);
        
        ts.assertValueCount(4)
        .assertComplete()
        .assertNoError();

        toList(ts.values().get(3))
        .assertValues(10)
        .assertComplete()
        .assertNoError();
    }
    
    @SafeVarargs
    static <T> void expect(TestSubscriber<Publisher<T>> ts, int index, T... values) {
        toList(ts.values().get(index))
        .assertValues(values)
        .assertComplete()
        .assertNoError();
    }
    
    @Test
    public void overlap() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        new PublisherWindow<>(new PublisherRange(1, 10), 3, 1, pqs, oqs).subscribe(ts);
        
        ts.assertValueCount(10)
        .assertComplete()
        .assertNoError();
        
        expect(ts, 0, 1, 2, 3);
        expect(ts, 1, 2, 3, 4);
        expect(ts, 2, 3, 4, 5);
        expect(ts, 3, 4, 5, 6);
        expect(ts, 4, 5, 6, 7);
        expect(ts, 5, 6, 7, 8);
        expect(ts, 6, 7, 8, 9);
        expect(ts, 7, 8, 9, 10);
        expect(ts, 8, 9, 10);
        expect(ts, 9, 10);
    }

    @Test
    public void overlapBackpressured() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>(0L);
        
        new PublisherWindow<>(new PublisherRange(1, 10), 3, 1, pqs, oqs).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        for (int i = 0; i < 10; i++) {
            ts.request(1);
            
            ts.assertValueCount(i + 1)
            .assertNoError();
            
            if (i == 9) {
                ts.assertComplete();
            } else {
                ts.assertNotComplete();
            }
            
            switch (i) {
            case 9:
                expect(ts, 9, 10);
                break;
            case 8:
                expect(ts, 8, 9, 10);
                break;
            case 7:
                expect(ts, 7, 8, 9, 10);
                break;
            case 6:
                expect(ts, 6, 7, 8, 9);
                break;
            case 5:
                expect(ts, 5, 6, 7, 8);
                break;
            case 4:
                expect(ts, 4, 5, 6, 7);
                break;
            case 3:
                expect(ts, 3, 4, 5, 6);
                break;
            case 2:
                expect(ts, 2, 3, 4, 5);
                break;
            case 1:
                expect(ts, 1, 2, 3, 4);
                break;
            case 0:
                expect(ts, 0, 1, 2, 3);
                break;
            }
        }
    }

    @Test
    public void exactError() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        new PublisherWindow<>(sp, 2, 2, pqs, oqs).subscribe(ts);
        
        ts.assertValueCount(0)
        .assertNotComplete()
        .assertNoError();
        
        sp.onNext(1);
        sp.onError(new RuntimeException("forced failure"));
        
        ts.assertValueCount(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
        
        toList(ts.values().get(0))
        .assertValue(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void skipError() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        new PublisherWindow<>(sp, 2, 3, pqs, oqs).subscribe(ts);
        
        ts.assertValueCount(0)
        .assertNotComplete()
        .assertNoError();
        
        sp.onNext(1);
        sp.onError(new RuntimeException("forced failure"));
        
        ts.assertValueCount(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
        
        toList(ts.values().get(0))
        .assertValue(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }

    @Test
    public void skipInGapError() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        new PublisherWindow<>(sp, 1, 3, pqs, oqs).subscribe(ts);
        
        ts.assertValueCount(0)
        .assertNotComplete()
        .assertNoError();
        
        sp.onNext(1);
        sp.onNext(2);
        sp.onError(new RuntimeException("forced failure"));
        
        ts.assertValueCount(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
        
        expect(ts, 0, 1);
    }
    
    @Test
    public void overlapError() {
        TestSubscriber<Publisher<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        new PublisherWindow<>(sp, 2, 1, pqs, oqs).subscribe(ts);
        
        ts.assertValueCount(0)
        .assertNotComplete()
        .assertNoError();
        
        sp.onNext(1);
        sp.onError(new RuntimeException("forced failure"));
        
        ts.assertValueCount(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
        
        toList(ts.values().get(0))
        .assertValue(1)
        .assertNotComplete()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure");
    }
}
