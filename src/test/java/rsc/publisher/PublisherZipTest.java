package rsc.publisher;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.*;

import org.junit.*;
import org.reactivestreams.Publisher;

import rsc.processor.UnicastProcessor;
import rsc.test.TestSubscriber;
import rsc.util.*;
import rsc.util.SpscArrayQueue;

public class PublisherZipTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherZip.class);
        
        ctb.addRef("sources", new Publisher[0]);
        ctb.addRef("sourcesIterable", Collections.emptyList());
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addRef("zipper", (Function<Object[], Object>)v -> v);
        
        ctb.test();
    }
    
    @Test
    public void sameLength() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source = Px.fromIterable(Arrays.asList(1, 2));
        source.zipWith(source, (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void sameLengthOptimized() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source = Px.range(1, 2);
        source.zipWith(source, (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void sameLengthBackpressured() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px<Integer> source = Px.fromIterable(Arrays.asList(1, 2));
        source.zipWith(source, (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);

        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        ts.request(2);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void sameLengthOptimizedBackpressured() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px<Integer> source = Px.range(1, 2);
        source.zipWith(source, (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);

        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        ts.request(2);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void differentLength() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.fromIterable(Arrays.asList(1, 2));
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void differentLengthOpt() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.range(1, 2);
        Px<Integer> source2 = Px.range(1, 3);
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void emptyNonEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.fromIterable(Collections.emptyList());
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void nonEmptyAndEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.fromIterable(Arrays.asList(1, 2, 3));
        Px<Integer> source2 = Px.fromIterable(Collections.emptyList());
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarNonScalar() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarNonScalarBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarNonScalarOpt() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.range(1, 3);
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarScalar() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.just(1);
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void emptyScalar() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.empty();
        Px<Integer> source2 = Px.just(1);
        source1.zipWith(source2, (a, b) -> a + b).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void sameLengthIterable() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source = Px.fromIterable(Arrays.asList(1, 2));
        
        Px.zipIterable(Arrays.asList(source, source), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void sameLengthOptimizedIterable() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source = Px.range(1, 2);
        Px.zipIterable(Arrays.asList(source, source), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void sameLengthBackpressuredIterable() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px<Integer> source = Px.fromIterable(Arrays.asList(1, 2));
        Px.zipIterable(Arrays.asList(source, source), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);

        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        ts.request(2);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void sameLengthOptimizedBackpressuredIterable() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px<Integer> source = Px.range(1, 2);
        Px.zipIterable(Arrays.asList(source, source), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);

        ts.assertValue(2)
        .assertNoError()
        .assertNotComplete();

        ts.request(2);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void differentLengthIterable() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.fromIterable(Arrays.asList(1, 2));
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void differentLengthOptIterable() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.range(1, 2);
        Px<Integer> source2 = Px.range(1, 3);
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertValues(2, 4)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void emptyNonEmptyIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.fromIterable(Collections.emptyList());
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void nonEmptyAndEmptyIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.fromIterable(Arrays.asList(1, 2, 3));
        Px<Integer> source2 = Px.fromIterable(Collections.emptyList());
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarNonScalarIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarNonScalarBackpressuredIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.fromIterable(Arrays.asList(1, 2, 3));
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarNonScalarOptIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.range(1, 3);
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void scalarScalarIterable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.just(1);
        Px<Integer> source2 = Px.just(1);
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void emptyScalarITerable() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px<Integer> source1 = Px.empty();
        Px<Integer> source2 = Px.just(1);
        Px.zipIterable(Arrays.asList(source1, source2), a -> (Integer)a[0] + (Integer)a[1]).subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void syncFusionMapToNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 10)
        .<Integer, Integer>zipWith(Px.range(1, 2).map(v -> v == 2 ? null : v), (a, b) -> a + b).subscribe(ts);
        
        ts.assertValue(2)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void syncFusionMapToNullFilter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 10)
        .<Integer, Integer>zipWith(Px.range(1, 2).map(v -> v == 2 ? null : v).filter(v -> true), (a, b) -> a + b).subscribe(ts);
        
        ts.assertValue(2)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void asyncFusionMapToNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(2));
        up.onNext(1);
        up.onNext(2);
        up.onComplete();
        
        Px.range(1, 10)
        .<Integer, Integer>zipWith(up.map(v -> v == 2 ? null : v), (a, b) -> a + b).subscribe(ts);
        
        ts.assertValue(2)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void asyncFusionMapToNullFilter() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(2));
        up.onNext(1);
        up.onNext(2);
        up.onComplete();

        Px.range(1, 10)
        .<Integer, Integer>zipWith(up.map(v -> v == 2 ? null : v).filter(v -> true), (a, b) -> a + b).subscribe(ts);
        
        ts.assertValue(2)
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void isEmptyFalseButPollFilters() {
        
        TestSubscriber<Object[]> ts = new TestSubscriber<>(0);
        
        Px<Integer> source = Px.fromArray(1, 2, 3).filter(v -> v == 2);
        
        Px.zip(a -> a, 1, source, source).subscribe(ts);
        
        ts.request(2);
        
        ts.assertValueCount(1)
        .assertNoError()
        .assertComplete();
        
        Assert.assertArrayEquals(new Object[] { 2, 2 }, ts.values().get(0));
        
        
    }
}
