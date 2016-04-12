package rsc.publisher;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import rsc.util.*;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='FlatMapFrontFusionPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class FlatMapFrontFusionPerf {
    
    @Param({"1", "10", "100", "1000", "1000000"})
    public int count;
    
    Publisher<Integer> source;
    
    @Setup
    public void setup() {
        Integer[] a = new Integer[count];
        Arrays.fill(a, 777);
        
        source = Px.fromArray(a).flatMap(Px::just);
    }
    @Benchmark
    public void unbounded(Blackhole bh) {
        source.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void bounded(Blackhole bh) {
        source.subscribe(new PerfSlowPathSubscriber(bh, count));
    }
}
