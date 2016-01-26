package reactivestreams.commons;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import reactivestreams.commons.internal.PerfSubscriber;
import reactivestreams.commons.publisher.PublisherBase;


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

    PublisherBase<Integer> baselineIterable;

    PublisherBase<Integer> baselineArray;

    PublisherBase<Integer> baselineRange;

    PublisherBase<Integer> zipIterable;

    PublisherBase<Integer> zipArray;
    
    PublisherBase<Integer> zipRange;
    
    @Param({"1", "1000", "1000000"})
    int count;
    @Setup
    public void setup() {
        Integer[] values = new Integer[count];
        Arrays.fill(values, 777);
        
        baselineIterable = PublisherBase.fromIterable(Arrays.asList(values));
        
        baselineArray = PublisherBase.fromArray(values);
        
        baselineRange = PublisherBase.range(1, count);
        
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
