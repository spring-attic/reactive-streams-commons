package reactivestreams.commons.publisher;

import java.util.function.BiFunction;

import org.junit.Test;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class PublisherAggregateTest {
    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherAggregate.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("aggregator", (BiFunction<Object, Object, Object>)(a, b) -> b);
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 10).aggregate((a, b) -> a + b).subscribe(ts);
        
        ts.assertValue(55)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0L);
        
        Px.range(1, 10).aggregate((a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValue(55)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void single() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.just(1).aggregate((a, b) -> a + b).subscribe(ts);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }


    @Test
    public void empty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.<Integer>empty().aggregate((a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.<Integer>error(new RuntimeException("forced failure")).aggregate((a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void aggregatorThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 10).aggregate((a, b) -> { throw new RuntimeException("forced failure"); }).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void aggregatorReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 10).aggregate((a, b) -> null).subscribe(ts);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

}
