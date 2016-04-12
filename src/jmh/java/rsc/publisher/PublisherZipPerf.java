package rsc.publisher;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import rsc.publisher.internal.PerfSubscriber;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherZipPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherZipPerf {

    Px<Integer> baselineIterable;

    Px<Integer> baselineArray;

    Px<Integer> baselineRange;

    Px<Integer> zipIterable;

    Px<Integer> zipArray;
    
    Px<Integer> zipRange;
    
    @Param({"1", "1000", "1000000"})
    int count;
    @Setup
    public void setup() {
        Integer[] values = new Integer[count];
        Arrays.fill(values, 777);
        
        baselineIterable = Px.fromIterable(Arrays.asList(values));
        
        baselineArray = Px.fromArray(values);
        
        baselineRange = Px.range(1, count);
        
        zipIterable = baselineIterable.zipWith(baselineIterable, (a, b) -> a + b);

        zipArray = baselineArray.zipWith(baselineArray, (a, b) -> a + b);

        zipRange = baselineRange.zipWith(baselineRange, (a, b) -> a + b);
    }
    
    @Benchmark
    public void baselineIterable(Blackhole bh) {
        baselineIterable.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void baselineArray(Blackhole bh) {
        baselineArray.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void baselineRange(Blackhole bh) {
        baselineRange.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void zipIterable(Blackhole bh) {
        zipIterable.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void zipArray(Blackhole bh) {
        zipArray.subscribe(new PerfSubscriber(bh));
    }
    
    @Benchmark
    public void zipRange(Blackhole bh) {
        zipRange.subscribe(new PerfSubscriber(bh));
    }
}
