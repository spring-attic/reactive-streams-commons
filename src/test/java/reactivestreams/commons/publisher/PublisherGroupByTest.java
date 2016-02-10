package reactivestreams.commons.publisher;

import java.util.function.Function;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherGroupByTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new  ConstructorTestBuilder(PublisherGroupBy.class);
        
        ctb.addRef("source", PublisherBase.never());
        ctb.addRef("keySelector", (Function<Object, Object>)v -> v);
        ctb.addRef("valueSelector", (Function<Object, Object>)v -> v);
        ctb.addRef("mainQueueSupplier", PublisherBase.defaultQueueSupplier(1));
        ctb.addRef("groupQueueSupplier", PublisherBase.defaultQueueSupplier(1));
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> k % 2).subscribe(ts);
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        ts.values().get(0).subscribe(ts1);
        ts1.assertResult(1, 3, 5, 7, 9);

        
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        ts.values().get(1).subscribe(ts2);
        ts2.assertResult(2, 4, 6, 8, 10);
    }

    @Test
    public void normalValueSelector() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> k % 2, v -> -v).subscribe(ts);
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        ts.values().get(0).subscribe(ts1);
        ts1.assertResult(-1, -3, -5, -7, -9);

        
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        ts.values().get(1).subscribe(ts2);
        ts2.assertResult(-2, -4, -6, -8, -10);
    }

    @Test
    public void takeTwoGroupsOnly() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> k % 3).take(2).subscribe(ts);
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        ts.values().get(0).subscribe(ts1);
        ts1.assertResult(1, 4, 7, 10);

        
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        ts.values().get(1).subscribe(ts2);
        ts2.assertResult(2, 5, 8);
    }

    @Test
    public void keySelectorNull() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> (Integer)null).subscribe(ts);
        
        ts.assertFailure(NullPointerException.class);
    }

    @Test
    public void valueSelectorNull() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> 1, v -> (Integer)null).subscribe(ts);
        
        ts.assertFailure(NullPointerException.class);
    }

    @Test
    public void error() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        PublisherBase.<Integer>error(new RuntimeException("forced failure")).groupBy(k -> k).subscribe(ts);
        
        ts.assertFailureMessage(RuntimeException.class, "forced failure");
    }

    @Test
    public void backpressure() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>(0L);
        
        PublisherBase.range(1, 10).groupBy(k -> 1).subscribe(ts);
        
        ts.assertNoEvents();
        
        ts.request(1);
        
        ts.assertValueCount(1)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0L);
        
        ts.values().get(0).subscribe(ts1);
        
        ts1.assertNoEvents();
        
        ts1.request(10);
        
        ts1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
    
    @Test
    public void flatMapBack() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> k % 2).flatMap(g -> g).subscribe(ts);
        
        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void flatMapBackHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> k % 2).flatMap(g -> g.hide()).subscribe(ts);
        
        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void concatMapBack() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> k % 2).concatMap(g -> g).subscribe(ts);
        
        ts.assertResult(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
    }

    @Test
    public void concatMapBackHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        PublisherBase.range(1, 10).groupBy(k -> k % 2).hide().concatMap(g -> g).subscribe(ts);
        
        ts.assertResult(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
    }
    
    @Test
    public void empty() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>(0L);

        PublisherBase.<Integer>empty().groupBy(v -> v).subscribe(ts);
        
        ts.assertResult();
    }
}
