package rsc.publisher;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import rsc.util.*;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherAggregatePerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherAggregatePerf {
    @Param({"1", "1000", "1000000"})
    public int count;
    
    Publisher<Integer> range;
    Publisher<Integer> array;
    
    @Setup
    public void setup() {
        range = Px.range(0, count).reduce(Integer::max);
        Integer[] arr = new Integer[count];
        for (int i = 0; i < count; i++) {
            arr[i] = i;
        }
        
        array = Px.fromArray(arr).reduce(Integer::max);
    }
    
    @Benchmark
    public void range(Blackhole bh) {
        range.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void array(Blackhole bh) {
        range.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeSlowpath(Blackhole bh) {
        range.subscribe(new PerfSlowPathSubscriber(bh, count));
    }

    @Benchmark
    public void arraySlowpath(Blackhole bh) {
        range.subscribe(new PerfSlowPathSubscriber(bh, count));
    }
}
