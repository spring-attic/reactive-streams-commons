package reactivestreams.commons;

import java.util.*;
import java.util.stream.Stream;

import org.junit.Test;

import reactivestreams.commons.PublisherStream;
import reactivestreams.commons.internal.subscribers.TestSubscriber;

public class PublisherStreamTest {

    final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    
    @Test(expected = NullPointerException.class)
    public void nullIterable() {
        new PublisherStream<Integer>(null);
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherStream<>(source.stream()).subscribe(ts);
        
        ts
        .assertValueSequence(source)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        new PublisherStream<>(source.stream()).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(5);
        
        ts
        .assertValues(1, 2, 3, 4, 5)
        .assertNotComplete()
        .assertNoError();
        
        ts.request(10);

        ts
        .assertValueSequence(source)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normalBackpressuredExact() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(10);
        
        new PublisherStream<>(source.stream()).subscribe(ts);
        
        ts
        .assertValueSequence(source)
        .assertComplete()
        .assertNoError();
    }
    
    @Test
    public void iteratorReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        new PublisherStream<>(Arrays.asList(1, 2, 3, 4, 5, null, 7, 8, 9, 10).stream()).subscribe(ts);
        
        ts
        .assertValues(1, 2, 3, 4, 5)
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void streamAlreadyConsumed() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Stream<Integer> s = source.stream();
        
        s.count();
        
        new PublisherStream<>(s).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(IllegalStateException.class);
    }
}
