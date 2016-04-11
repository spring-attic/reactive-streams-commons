package reactivestreams.commons.publisher;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import reactivestreams.commons.publisher.internal.PerfSubscriber;

/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='CrossRangeFlatMapObserveOnPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
@Timeout(time = 10, timeUnit = TimeUnit.SECONDS)
public class PublisherReducePerf {
    @Param({"1", "10", "100", "1000", "10000", "100000", "1000000" })
    public int count;
    
    Px<Integer> aggregatedBaseline;

    Px<Integer> aggregatedFused;
    
    Px<Integer> reducedBaseline;

    Px<Integer> reducedFused;

    @Setup
    public void setup() {
        Integer[] values = new Integer[count];
        Arrays.fill(values, 777);
        
        Px<Integer> source = Px.fromArray(values);
        
        aggregatedBaseline = source.aggregate((u, v) -> u + v);
        
        reducedBaseline = source.reduce(() -> 0, (u, v) -> u + v);
        
        aggregatedFused = Px.just(1).hide().flatMap(v -> aggregatedBaseline);

        reducedFused = Px.just(1).hide().flatMap(v -> reducedBaseline);
    }
    
    @Benchmark
    public void aggregateBaseline(Blackhole bh) {
        aggregatedBaseline.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void aggregateFused(Blackhole bh) {
        aggregatedFused.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void reduceBaseline(Blackhole bh) {
        reducedBaseline.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void reduceFused(Blackhole bh) {
        reducedFused.subscribe(new PerfSubscriber(bh));
    }

}
