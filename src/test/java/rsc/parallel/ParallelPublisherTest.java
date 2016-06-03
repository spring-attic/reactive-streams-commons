package rsc.parallel;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import rsc.processor.DirectProcessor;
import rsc.publisher.Px;
import rsc.scheduler.*;
import rsc.test.TestSubscriber;

public class ParallelPublisherTest {

    @Test
    public void sequentialMode() {
        Px<Integer> source = Px.range(1, 1_000_000).hide();
        for (int i = 1; i < 33; i++) {
            Px<Integer> result = ParallelPublisher.fork(source, false, i)
            .map(v -> v + 1)
            .join()
            ;
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            result.subscribe(ts);

            ts
            .assertSubscribed()
            .assertValueCount(1_000_000)
            .assertComplete()
            .assertNoError()
            ;
        }
        
    }

    @Test
    public void sequentialModeFused() {
        Px<Integer> source = Px.range(1, 1_000_000);
        for (int i = 1; i < 33; i++) {
            Px<Integer> result = ParallelPublisher.fork(source, false, i)
            .map(v -> v + 1)
            .join()
            ;
            
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            result.subscribe(ts);

            ts
            .assertSubscribed()
            .assertValueCount(1_000_000)
            .assertComplete()
            .assertNoError()
            ;
        }
        
    }

    @Test
    public void parallelMode() {
        Px<Integer> source = Px.range(1, 1_000_000).hide();
        int ncpu = Math.max(8, Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < ncpu + 1; i++) {
            
            Scheduler scheduler = new ParallelScheduler(i);
            
            try {
                Px<Integer> result = ParallelPublisher.fork(source, false, i)
                .runOn(scheduler)
                .map(v -> v + 1)
                .join()
                ;
                
                TestSubscriber<Integer> ts = new TestSubscriber<>();
                
                result.subscribe(ts);
    
                ts.assertTerminated(10, TimeUnit.SECONDS);
                
                ts
                .assertSubscribed()
                .assertValueCount(1_000_000)
                .assertComplete()
                .assertNoError()
                ;
            } finally {
                scheduler.shutdown();
            }
        }
        
    }

    @Test
    public void parallelModeFused() {
        Px<Integer> source = Px.range(1, 1_000_000);
        int ncpu = Math.max(8, Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < ncpu + 1; i++) {
            
            Scheduler scheduler = new ParallelScheduler(i);
            
            try {
                Px<Integer> result = ParallelPublisher.fork(source, false, i)
                .runOn(scheduler)
                .map(v -> v + 1)
                .join()
                ;
                
                TestSubscriber<Integer> ts = new TestSubscriber<>();
                
                result.subscribe(ts);
    
                ts.assertTerminated(10, TimeUnit.SECONDS);
                
                ts
                .assertSubscribed()
                .assertValueCount(1_000_000)
                .assertComplete()
                .assertNoError()
                ;
            } finally {
                scheduler.shutdown();
            }
        }
        
    }

    @Test
    public void reduceFull() {
        for (int i = 1; i <= Runtime.getRuntime().availableProcessors() * 2; i++) {
            TestSubscriber<Integer> ts = new TestSubscriber<>();
            
            Px.range(1, 10)
            .parallel(i)
            .reduce((a, b) -> a + b)
            .subscribe(ts);
            
            ts.assertResult(55);
        }
    }
    
    @Test
    public void parallelReduceFull() {
        int m = 100_000;
        for (int n = 1; n <= m; n *= 10) {
//            System.out.println(n);
            for (int i = 1; i <= Runtime.getRuntime().availableProcessors(); i++) {
//                System.out.println("  " + i);
                
                ParallelScheduler scheduler = new ParallelScheduler(i);
                
                try {
                    TestSubscriber<Long> ts = new TestSubscriber<>();
                    
                    Px.range(1, n)
                    .map(v -> (long)v)
                    .parallel(i)
                    .runOn(scheduler)
                    .reduce((a, b) -> a + b)
                    .subscribe(ts);
        
                    ts.assertTerminated(500, TimeUnit.SECONDS);
                    
                    long e = ((long)n) * (1 + n) / 2;
                    
                    ts.assertResult(e);
                } finally {
                    scheduler.shutdown();
                }
            }
        }
    }
    
    @Test
    public void toSortedList() {
        TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
        
        Px.fromArray(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
        .parallel()
        .toSortedList(Comparator.naturalOrder())
        .subscribe(ts);
        
        ts.assertResult(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    }
    
    @Test
    public void sorted() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Px.fromArray(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
        .parallel()
        .sorted(Comparator.naturalOrder())
        .subscribe(ts);
        
        ts.assertNoValues();
        
        ts.request(2);
        
        ts.assertValues(1, 2);
        
        ts.request(5);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7);
        
        ts.request(3);

        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
    
    @Test
    public void collect() {
        Supplier<List<Integer>> as = () -> new ArrayList<>();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        Px.range(1, 10)
        .parallel()
        .collect(as, (a, b) -> a.add(b))
        .sequential()
        .flatMapIterable(v -> v)
        .subscribe(ts);
        
        ts.assertValueSet(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void streamCollect() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 10)
        .parallel()
        .collect(Collectors.toList())
        .sequential()
        .flatMapIterable(v -> v)
        .subscribe(ts);
        
        ts.assertValueSet(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void groupMerge() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 10)
        .parallel()
        .groups()
        .flatMap(v -> v)
        .subscribe(ts);
        
        ts.assertValueSet(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void from() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        ParallelPublisher.from(Px.range(1, 5), Px.range(6, 5))
        .sequential()
        .subscribe(ts);
        
        ts.assertValueSet(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void orderedSourceMapJoin() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 10)
        .parallel(true)
        .map(v -> v + 1)
        .sequential()
        .subscribe(ts);
        
        ts.assertResult(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    }
    
    @Test
    public void orderedSourceFilterJoin() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        Px.range(1, 10)
        .parallel(true)
        .filter(v -> (v & 1) != 0)
        .sequential()
        .subscribe(ts);
        
        ts.assertResult(1, 3, 5, 7, 9);
    }
    
    @Test
    public void concatMapOrdered() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        
        dp
        .parallel(true)
        .concatMap(v -> Px.range(v * 10 + 1, 3))
        .sequential()
        .subscribe(ts);
        
        dp.onNext(1);
        dp.onNext(2);
        dp.onNext(3);
        dp.onNext(4);
        dp.onNext(5);
        dp.onComplete();
        
        ts.assertResult(11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53);
    }
    
    @Test
    public void concatMapUnordered() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 5)
        .parallel()
        .concatMap(v -> Px.range(v * 10 + 1, 3))
        .sequential()
        .subscribe(ts);
        
        ts.assertValueSet(new HashSet<>(Arrays.asList(11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53)))
        .assertNoError()
        .assertComplete();
        
    }
    
    @Test
    public void flatMapUnordered() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 5)
        .parallel()
        .flatMap(v -> Px.range(v * 10 + 1, 3))
        .sequential()
        .subscribe(ts);
        
        ts.assertValueSet(new HashSet<>(Arrays.asList(11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53)))
        .assertNoError()
        .assertComplete();
        
    }
}