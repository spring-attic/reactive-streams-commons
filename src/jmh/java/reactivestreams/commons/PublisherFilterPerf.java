package reactivestreams.commons;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;

import reactivestreams.commons.internal.*;
import reactivestreams.commons.publisher.PublisherBase;


/**
 * Example benchmark. Run from command line as
 * <br>
 * gradle jmh -Pjmh='PublisherFilterPerf'
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class PublisherFilterPerf {
    
    Publisher<Integer> simple;

    Publisher<Integer> rangeFlatMapRange;
    
    Publisher<Integer> rangeConcatMapRange;
    
    @Setup
    public void setup() {
        simple = PublisherBase.range(1, 1_000_000).filter(v -> (v & 1) == 0);
        
        rangeFlatMapRange = PublisherBase.range(1, 1000).flatMap(v -> PublisherBase.range(1, 1000).filter(w -> (w & 1) == 0));

        rangeConcatMapRange = PublisherBase.range(1, 1000).concatMap(v -> PublisherBase.range(1, 1000).filter(w -> (w & 1) == 0));
    }
    
    @Benchmark
    public void simple(Blackhole bh) {
        simple.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeFlatMapRange(Blackhole bh) {
        rangeFlatMapRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void rangeConcatMapRange(Blackhole bh) {
        rangeConcatMapRange.subscribe(new PerfSubscriber(bh));
    }

    @Benchmark
    public void simpleSlowpath(Blackhole bh) {
        simple.subscribe(new PerfSlowPathSubscriber(bh, 2_000_000));
    }

    @Benchmark
    public void rangeFlatMapRangeSlowpath(Blackhole bh) {
        rangeFlatMapRange.subscribe(new PerfSlowPathSubscriber(bh, 2_000_000));
    }

    @Benchmark
    public void rangeConcatMapRangeSlowpath(Blackhole bh) {
        rangeConcatMapRange.subscribe(new PerfSlowPathSubscriber(bh, 2_000_000));
    }

}
