package rsc.publisher;

import java.util.Arrays;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import rsc.test.TestSubscriber;
import rsc.util.ConstructorTestBuilder;

public class PublisherConcatMapIterableTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherFlattenIterable.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("mapper", (Function<Object, Iterable<Object>>)v -> Collections.emptyList());
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 5).concatMapIterable(v -> Arrays.asList(v, v + 1))
          .subscribe(ts);
        
        ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 5).concatMapIterable(v -> Arrays.asList(v, v + 1))
        .subscribe(ts);
        
        ts.assertNoEvents();
        
        ts.request(1);
        
        ts.assertIncomplete(1);
        
        ts.request(2);
        
        ts.assertIncomplete(1, 2, 2);
        
        ts.request(7);
        
        ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalNoFusion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 5).hide().concatMapIterable(v -> Arrays.asList(v, v + 1))
        .subscribe(ts);
        
        ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressuredNoFusion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.range(1, 5).hide().concatMapIterable(v -> Arrays.asList(v, v + 1))
        .subscribe(ts);
        
        ts.assertNoEvents();
        
        ts.request(1);
        
        ts.assertIncomplete(1);
        
        ts.request(2);
        
        ts.assertIncomplete(1, 2, 2);
        
        ts.request(7);
        
        ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void longRunning() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        int n = 1_000_000;
        
        Px.range(1, n).concatMapIterable(v -> Arrays.asList(v, v + 1))
        .subscribe(ts);

        ts.assertValueCount(n * 2)
        .assertNoError()
        .assertComplete()
        ;
    }

    @Test
    public void longRunningNoFusion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        int n = 1_000_000;
        
        Px.range(1, n).hide().concatMapIterable(v -> Arrays.asList(v, v + 1))
        .subscribe(ts);

        ts.assertValueCount(n * 2)
        .assertNoError()
        .assertComplete()
        ;
    }

    @Test
    public void fullFusion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        int n = 1_000_000;
        
        Px.range(1, n).concatMapIterable(v -> Arrays.asList(v, v + 1)).concatMap(Px::just)
        .subscribe(ts);

        ts.assertValueCount(n * 2)
        .assertNoError()
        .assertComplete()
        ;
    }

    @Test
    public void just() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.just(1).concatMapIterable(v -> Arrays.asList(v, v + 1))
        .subscribe(ts);
        
        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.<Integer>empty().concatMapIterable(v -> Arrays.asList(v, v + 1))
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

}
