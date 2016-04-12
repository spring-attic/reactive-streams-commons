package rsc.publisher;

import java.util.Arrays;
import java.util.concurrent.*;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import rsc.publisher.internal.PerfAsyncSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherObserveOnPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherObserveOnPerf {

    @Param({"1", "1000", "1000000"})
    int count;

    Publisher<Integer> range;
    Publisher<Integer> rangeHidden;

    Publisher<Integer> array;
    Publisher<Integer> arrayHidden;

    Publisher<Integer> iterable;
    Publisher<Integer> iterableHidden;

    ExecutorService exec;
    
    @Setup
    public void setup(Blackhole bh) {
        exec = Executors.newSingleThreadExecutor();
        
        Px<Integer> source = Px.range(1, count);
        
        range = source.observeOn(exec);
        rangeHidden = source.hide().observeOn(exec);
        
        Integer[] a = new Integer[count];
        Arrays.fill(a, 777);

        Px<Integer> arr = Px.fromArray(a);
        
        array = arr.observeOn(exec);
        arrayHidden = arr.hide().observeOn(exec);
        
        Px<Integer> it = Px.fromIterable(Arrays.asList(a));
        
        iterable = it.observeOn(exec);
        iterableHidden = it.hide().observeOn(exec);
    }
    
    @TearDown
    public void teardown() {
        exec.shutdownNow();
    }
    
    void run(Publisher<Integer> p, Blackhole bh) {
        PerfAsyncSubscriber s = new PerfAsyncSubscriber(bh);
        
        p.subscribe(s);
        
        s.await(count);
    }
    
    @Benchmark
    public void range(Blackhole bh) {
        run(range, bh);
    }

    @Benchmark
    public void rangeHidden(Blackhole bh) {
        run(rangeHidden, bh);
    }

    @Benchmark
    public void array(Blackhole bh) {
        run(array, bh);
    }

    @Benchmark
    public void arrayHidden(Blackhole bh) {
        run(arrayHidden, bh);
    }

    @Benchmark
    public void iterable(Blackhole bh) {
        run(iterable, bh);
    }

    @Benchmark
    public void iterableHidden(Blackhole bh) {
        run(iterableHidden, bh);
    }
}
